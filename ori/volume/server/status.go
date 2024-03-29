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

package server

import (
	"context"
	"fmt"

	ori "github.com/onmetal/onmetal-api/ori/apis/volume/v1alpha1"
)

func (s *Server) Status(ctx context.Context, req *ori.StatusRequest) (*ori.StatusResponse, error) {
	log := s.loggerFrom(ctx)
	log.V(1).Info("Volume Status called")

	log.V(1).Info("Listing onmetal volume classes")
	volumeClassList := s.volumeClasses.List()

	log.V(1).Info("Getting ceph pool stats")
	poolStats, err := s.cephCommandClient.PoolStats()
	if err != err {
		return nil, fmt.Errorf("failed to get ceph pool stats: %w", err)
	}

	var volumeClassStatus []*ori.VolumeClassStatus
	for _, volumeClass := range volumeClassList {
		volumeClassStatus = append(volumeClassStatus, &ori.VolumeClassStatus{
			VolumeClass: volumeClass,
			Quantity:    poolStats.MaxAvail,
		})
	}

	log.V(1).Info("Returning status with volume classes")
	return &ori.StatusResponse{
		VolumeClassStatus: volumeClassStatus,
	}, nil
}
