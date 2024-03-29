// Copyright 2023 OnMetal authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"encoding/json"

	"github.com/onmetal/cephlet/ori/volume/apiutils"
	"github.com/onmetal/cephlet/pkg/api"
	"github.com/onmetal/cephlet/pkg/omap"
	metav1alpha1 "github.com/onmetal/onmetal-api/ori/apis/meta/v1alpha1"
	oriv1alpha1 "github.com/onmetal/onmetal-api/ori/apis/volume/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Volume Status", func() {
	It("should get the supported volume class status", func(ctx SpecContext) {
		By("getting volume status")
		resp, err := volumeClient.Status(ctx, &oriv1alpha1.StatusRequest{})
		Expect(err).NotTo(HaveOccurred())

		By("validating volume class status")
		Expect(resp.VolumeClassStatus[0]).Should(SatisfyAll(
			HaveField("VolumeClass", Equal(&oriv1alpha1.VolumeClass{
				Name: "foo",
				Capabilities: &oriv1alpha1.VolumeClassCapabilities{
					Tps:  100,
					Iops: 100,
				},
			})),
			// TODO: The pool size depends on the ceph setup in the integration test workflow.
			// We need to adjust/make the pool size configurable in the future.
			HaveField("Quantity", And(
				BeNumerically(">", int64(9*1024*1024*1024)),
				BeNumerically("<=", int64(14*1024*1024*1024)),
			)),
		))

		By("creating a volume with the given volume class")
		createResp, err := volumeClient.CreateVolume(ctx, &oriv1alpha1.CreateVolumeRequest{
			Volume: &oriv1alpha1.Volume{
				Metadata: &metav1alpha1.ObjectMetadata{
					Id: "foo",
				},
				Spec: &oriv1alpha1.VolumeSpec{
					Class: "foo",
					Resources: &oriv1alpha1.VolumeResources{
						StorageBytes: 1 * 1024,
					},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(createResp).Should(SatisfyAll(
			HaveField("Volume.Metadata.Id", Not(BeEmpty())),
			HaveField("Volume.Spec.Class", Equal("foo")),
		))

		By("ensuring correct iops and tps/bps in Ceph cluster image specs")
		image := &api.Image{}
		Eventually(func() *api.Image {
			oMap, err := ioctx.GetOmapValues(omap.OmapNameVolumes, "", createResp.Volume.Metadata.Id, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(oMap).To(HaveKey(createResp.Volume.Metadata.Id))
			Expect(json.Unmarshal(oMap[createResp.Volume.Metadata.Id], image)).NotTo(HaveOccurred())
			return image
		}).Should(SatisfyAll(
			HaveField("Metadata.ID", Equal(createResp.Volume.Metadata.Id)),
			HaveField("Metadata.Labels", HaveKeyWithValue(apiutils.ClassLabel, "foo")),
			HaveField("Spec.Size", Equal(uint64(1*1024))),
			HaveField("Spec.Limits", SatisfyAll(
				HaveKeyWithValue(api.ReadBPSLimit, int64(100)),
				HaveKeyWithValue(api.WriteBPSLimit, int64(100)),
				HaveKeyWithValue(api.BPSLimit, int64(100)),
				HaveKeyWithValue(api.ReadIOPSLimit, int64(100)),
				HaveKeyWithValue(api.WriteIOPSLimit, int64(100)),
				HaveKeyWithValue(api.IOPSlLimit, int64(100)),
			)),
		))

		DeferCleanup(volumeClient.DeleteVolume, &oriv1alpha1.DeleteVolumeRequest{
			VolumeId: createResp.Volume.Metadata.Id,
		})
	})
})
