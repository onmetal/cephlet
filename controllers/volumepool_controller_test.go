// Copyright 2022 OnMetal authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"github.com/onmetal/cephlet/pkg/rook"
	"github.com/onmetal/controller-utils/clientutils"
	storagev1alpha1 "github.com/onmetal/onmetal-api/api/storage/v1alpha1"
	"github.com/onmetal/onmetal-api/testutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	rookv1 "github.com/rook/rook/pkg/apis/ceph.rook.io/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("VolumePoolReconciler", func() {
	ctx := testutils.SetupContext()
	testNs, rookNs, _ := SetupTest(ctx)

	When("is started", func() {
		It("should announce a VolumePool", func() {
			By("checking that a VolumePool has been created")
			volumePool := &storagev1alpha1.VolumePool{}
			volumePoolKey := types.NamespacedName{Name: volumePoolName}
			Eventually(func() error { return k8sClient.Get(ctx, volumePoolKey, volumePool) }).Should(Succeed())

			By("checking that a CephBlockPool has been created")
			rookPool := &rookv1.CephBlockPool{}
			rookPoolKey := types.NamespacedName{Name: volumePoolName, Namespace: rookNs.Name}
			Eventually(func() error { return k8sClient.Get(ctx, rookPoolKey, rookPool) }).Should(Succeed())

			Expect(rookPool.Spec.PoolSpec.Replicated.Size).To(HaveValue(Equal(uint(volumePoolReplication))))
			Expect(rookPool.Spec.PoolSpec.EnableRBDStats).To(Equal(rook.EnableRBDStatsDefaultValue))

			By("checking that a VolumePool reflect the rook status")
			rookPoolBase := rookPool.DeepCopy()
			rookPool.Status = &rookv1.CephBlockPoolStatus{
				Phase: rookv1.ConditionProgressing,
			}
			Expect(k8sClient.Status().Patch(ctx, rookPool, client.MergeFrom(rookPoolBase))).To(Succeed())

			Eventually(func(g Gomega) error {
				if err := k8sClient.Get(ctx, volumePoolKey, volumePool); err != nil {
					return err
				}
				g.Expect(volumePool.Status.State).To(BeEquivalentTo(storagev1alpha1.VolumePoolStatePending))
				return nil
			}).Should(Succeed())

			By("checking that the ceph client has been created and updating it to ready")
			cephClient := &rookv1.CephClient{}
			cephClientKey := types.NamespacedName{Name: GetClusterPoolName(rookConfig.ClusterId, volumePoolName), Namespace: rookNs.Name}
			cephClientSecret := &corev1.Secret{}
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, cephClientKey, cephClient)).To(Succeed())

				cephClientSecret = getCephClientSecret(rookNs.Name, GetClusterPoolName(rookConfig.ClusterId, volumePoolName), cephClientSecretValue)
				g.Expect(clientutils.IgnoreAlreadyExists(k8sClient.Create(ctx, cephClientSecret))).To(Succeed())

				cephClientBase := cephClient.DeepCopy()
				cephClient.Status = &rookv1.CephClientStatus{
					Phase: rookv1.ConditionReady,
					Info: map[string]string{
						cephClientSecretKey: cephClientSecret.Name,
					},
				}
				g.Expect(k8sClient.Status().Patch(ctx, cephClient, client.MergeFrom(cephClientBase))).To(Succeed())
			}).Should(Succeed())

			By("checking that a VolumePool annotations has been updated")
			Eventually(func(g Gomega) error {
				if err := k8sClient.Get(ctx, volumePoolKey, volumePool); err != nil {
					return err
				}
				g.Expect(volumePool.Annotations[volumePoolSecretAnnotation]).To(BeEquivalentTo(cephClientSecret.Name))
				return nil
			}).Should(Succeed())

			By("checking that a StorageClass has been created")
			storageClass := &storagev1.StorageClass{}
			storageClassKey := types.NamespacedName{Name: GetClusterPoolName(rookConfig.ClusterId, volumePoolName)}
			Eventually(func() error { return k8sClient.Get(ctx, storageClassKey, storageClass) }).Should(Succeed())

			Expect(storageClass.Provisioner).To(BeEquivalentTo(rookConfig.CSIDriverName))

			By("checking that a VolumeSnapshotClass has been created")
			volumeSnapshotClass := &snapshotv1.VolumeSnapshotClass{}
			volumeSnapshotClassKey := types.NamespacedName{Name: GetClusterPoolName(rookConfig.ClusterId, volumePoolName)}
			Eventually(func() error { return k8sClient.Get(ctx, volumeSnapshotClassKey, volumeSnapshotClass) }).Should(Succeed())

			Expect(volumeSnapshotClass.Driver).To(BeEquivalentTo(rookConfig.CSIDriverName))

			By("checking that a VolumePool reflect the rook status")
			rookPoolBase = rookPool.DeepCopy()
			rookPool.Status.Phase = rookv1.ConditionFailure
			Expect(k8sClient.Status().Patch(ctx, rookPool, client.MergeFrom(rookPoolBase))).To(Succeed())

			Eventually(func(g Gomega) error {
				if err := k8sClient.Get(ctx, volumePoolKey, volumePool); err != nil {
					return err
				}
				g.Expect(volumePool.Status.State).To(BeEquivalentTo(storagev1alpha1.VolumePoolStateNotAvailable))
				return nil
			}).Should(Succeed())

			rookPoolBase = rookPool.DeepCopy()
			rookPool.Status.Phase = rookv1.ConditionReady
			Expect(k8sClient.Status().Patch(ctx, rookPool, client.MergeFrom(rookPoolBase))).To(Succeed())

			Eventually(func(g Gomega) error {
				if err := k8sClient.Get(ctx, volumePoolKey, volumePool); err != nil {
					return err
				}
				g.Expect(volumePool.Status.State).To(BeEquivalentTo(storagev1alpha1.VolumePoolStateAvailable))
				g.Expect(volumePool.Status.AvailableVolumeClasses).To(BeNil())
				return nil
			}).Should(Succeed())

			By("creating a VolumeClass")
			volumeClass := &storagev1alpha1.VolumeClass{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "sc-",
					Labels:       volumeClassSelector,
				},
				Capabilities: map[corev1.ResourceName]resource.Quantity{
					storagev1alpha1.ResourceIOPS: resource.MustParse("100"),
					storagev1alpha1.ResourceTPS:  resource.MustParse("1"),
				},
			}
			Expect(k8sClient.Create(ctx, volumeClass)).To(Succeed())

			By("creating a second VolumeClass")
			Expect(k8sClient.Create(ctx, &storagev1alpha1.VolumeClass{
				TypeMeta: metav1.TypeMeta{},
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "sc-",
					Labels: map[string]string{
						"suitable-for": "production",
					},
				},
				Capabilities: map[corev1.ResourceName]resource.Quantity{
					storagev1alpha1.ResourceIOPS: resource.MustParse("100"),
					storagev1alpha1.ResourceTPS:  resource.MustParse("1"),
				},
			})).To(Succeed())

			By("checking that the VolumePool status includes the correct VolumeClass")
			Eventually(func(g Gomega) error {
				if err := k8sClient.Get(ctx, volumePoolKey, volumePool); err != nil {
					return err
				}
				g.Expect(volumePool.Status.State).To(BeEquivalentTo(storagev1alpha1.VolumePoolStateAvailable))
				g.Expect(volumePool.Status.AvailableVolumeClasses).To(HaveLen(1))
				g.Expect(volumePool.Status.AvailableVolumeClasses).To(ContainElement(corev1.LocalObjectReference{Name: volumeClass.Name}))
				return nil
			}).Should(Succeed())
		})
	})

	When("should reconcile", func() {
		It("a valid custom created pool", func() {
			volumePool := &storagev1alpha1.VolumePool{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "custom-pool-",
					Namespace:    testNs.Name,
				},
				Spec: storagev1alpha1.VolumePoolSpec{
					ProviderID: "custom://custom-pool",
				},
			}
			Expect(k8sClient.Create(ctx, volumePool)).Should(Succeed())

			By("checking that a VolumePool has not been created")
			rookPool := &rookv1.CephBlockPool{}
			rookPoolKey := types.NamespacedName{Name: volumePool.Name, Namespace: rookNs.Name}
			Eventually(func() bool { return errors.IsNotFound(k8sClient.Get(ctx, rookPoolKey, rookPool)) }).Should(BeTrue())
		})
	})
})

func getCephClientSecret(rookNs, dataKey, secret string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "secret-",
			Namespace:    rookNs,
		},
		Data: map[string][]byte{
			dataKey: []byte(secret),
		},
	}
}
