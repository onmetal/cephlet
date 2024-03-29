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

package server

import (
	"context"

	oriv1alpha1 "github.com/onmetal/onmetal-api/ori/apis/bucket/v1alpha1"
)

func (s *Server) ListBucketClasses(ctx context.Context, req *oriv1alpha1.ListBucketClassesRequest) (*oriv1alpha1.ListBucketClassesResponse, error) {
	log := s.loggerFrom(ctx)
	log.V(1).Info("Listing bucket classes")

	classes := s.bucketClassess.List()

	log.V(1).Info("Returning bucket classes")
	return &oriv1alpha1.ListBucketClassesResponse{
		BucketClasses: classes,
	}, nil
}
