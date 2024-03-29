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
	"fmt"
	"strings"

	"github.com/onmetal/cephlet/pkg/api"
	"github.com/onmetal/cephlet/pkg/omap"
	metav1alpha1 "github.com/onmetal/onmetal-api/ori/apis/meta/v1alpha1"
	oriv1alpha1 "github.com/onmetal/onmetal-api/ori/apis/volume/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Delete Volume", func() {
	It("should delete a volume", func(ctx SpecContext) {
		By("creating a volume")
		createResp, err := volumeClient.CreateVolume(ctx, &oriv1alpha1.CreateVolumeRequest{
			Volume: &oriv1alpha1.Volume{
				Metadata: &metav1alpha1.ObjectMetadata{
					Id: "foo",
				},
				Spec: &oriv1alpha1.VolumeSpec{
					Class: "foo",
					Resources: &oriv1alpha1.VolumeResources{
						StorageBytes: 1024 * 1024 * 1024,
					},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred())

		By("ensure the correct image has been created inside the ceph cluster")
		image := &api.Image{}
		Eventually(func() *api.Image {
			oMap, err := ioctx.GetOmapValues(omap.OmapNameVolumes, "", createResp.Volume.Metadata.Id, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(oMap).To(HaveKey(createResp.Volume.Metadata.Id))
			Expect(json.Unmarshal(oMap[createResp.Volume.Metadata.Id], image)).NotTo(HaveOccurred())
			return image
		}).Should(SatisfyAll(
			HaveField("Metadata.ID", Equal(createResp.Volume.Metadata.Id)),
			HaveField("Status.State", Equal(api.ImageStateAvailable)),
			HaveField("Status.Access", SatisfyAll(
				HaveField("Monitors", cephMonitors),
				HaveField("Handle", fmt.Sprintf("%s/%s", cephPoolname, "img_"+createResp.Volume.Metadata.Id)),
				HaveField("User", strings.TrimPrefix(cephClientname, "client.")),
				HaveField("UserKey", Not(BeEmpty())),
			)),
			HaveField("Status.Encryption", api.EncryptionState("")),
		))

		By("deleting a volume")
		Eventually(func() {
			_, err = volumeClient.DeleteVolume(ctx, &oriv1alpha1.DeleteVolumeRequest{
				VolumeId: createResp.Volume.Metadata.Id,
			})
			Expect(err).NotTo(HaveOccurred())
		})

		By("listing volume with volume ID to check volume deleted")
		Eventually(func() {
			resp, err := volumeClient.ListVolumes(ctx, &oriv1alpha1.ListVolumesRequest{
				Filter: &oriv1alpha1.VolumeFilter{
					Id: createResp.Volume.Metadata.Id,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.Volumes).To(BeEmpty())
		})

		By("ensuring the image has been deleted inside the ceph cluster")
		Eventually(func() {
			oMap, err := ioctx.GetOmapValues(omap.OmapNameVolumes, "", createResp.Volume.Metadata.Id, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(oMap).NotTo(HaveKey(createResp.Volume.Metadata.Id))
		})
	})
})
