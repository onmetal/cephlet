package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/ceph/go-ceph/rados" // <-- Import rados
	"github.com/go-logr/logr"
	"github.com/onmetal/cephlet/ori/volume/apiutils"
	"github.com/onmetal/cephlet/pkg/store"
	ori "github.com/onmetal/onmetal-api/ori/apis/volume/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/labels"

	volumev1alpha1 "github.com/onmetal/cephlet/ori/volume/api/v1alpha1" // <-- Ensure this is imported
	"github.com/onmetal/cephlet/ori/volume/apiutils"
	cephapi "github.com/onmetal/cephlet/pkg/api" // <-- Import ceph api
	"github.com/onmetal/cephlet/pkg/store"
)

// imageListerWithLabels defines the interface required by the ListVolumes function,
// explicitly listing all methods from store.Store and adding ListByLabels.
type imageListerWithLabels interface {
	// Methods from store.Store[*cephapi.Image]
	Create(ctx context.Context, obj *cephapi.Image) (*cephapi.Image, error)
	Get(ctx context.Context, id string) (*cephapi.Image, error)
	Update(ctx context.Context, obj *cephapi.Image) (*cephapi.Image, error)
	Delete(ctx context.Context, id string) error
	List(ctx context.Context) ([]*cephapi.Image, error)
	Watch(ctx context.Context) (store.Watch[*cephapi.Image], error)

	// New method for label-based listing
	ListByLabels(ctx context.Context, labelSelector map[string]string) ([]*cephapi.Image, error)
}

func (s *Server) getOriVolume(ctx context.Context, log logr.Logger, imageId string) (*ori.Volume, error) {
	// Type assertion to check if the store supports ListByLabels.
	// This is mainly for type safety, assuming imageStore is indeed the omap.Store.
	_, ok := s.imageStore.(imageListerWithLabels) // Check remains the same, but the interface definition is fixed
	if !ok {
		// Fallback or error if the store doesn't support the required method.
		// For now, let's proceed with the original Get, but log a warning.
		log.V(0).Info("Warning: imageStore does not implement ListByLabels, falling back to Get")
		// If ListByLabels is crucial, you might return an error here instead.
	}

	cephImage, err := s.imageStore.Get(ctx, imageId) // Use the original Get method
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Check if the error is specifically the omap not found error
			if errors.Is(err, rados.ErrNotFound) { // Import "github.com/ceph/go-ceph/rados" if needed
				log.V(1).Info("OMAP not found for volume", "volumeID", imageId)
				return nil, status.Errorf(codes.NotFound, "volume %s not found (omap)", imageId)
			}
			log.V(1).Info("Volume not found in store", "volumeID", imageId)
			return nil, status.Errorf(codes.NotFound, "volume %s not found", imageId)
		}
		log.Error(err, "Failed to get volume from store", "volumeID", imageId)
		return nil, fmt.Errorf("failed to get image: %w", err)
	}

	if !apiutils.IsManagedBy(cephImage, apiutils.VolumeManager) {
		return nil, status.Errorf(codes.NotFound, "image %s not found", imageId)
	}

	return s.convertImageToOriVolume(cephImage)
}

// filterVolumes can potentially be removed if ListByLabels covers the main filtering case.
// However, it might still be needed if other filter types are added later.
// For now, keep it but note it won't be called for pure label filtering.
func (s *Server) filterVolumes(volumes []*ori.Volume, filter *ori.VolumeFilter) []*ori.Volume {
	if filter == nil || (filter.Id == "" && len(filter.LabelSelector) == 0) {
		return volumes
	}

	var (
		res []*ori.Volume
		sel labels.Selector // Initialize sel outside the loop
	)

	// Create selector only if LabelSelector is present
	if len(filter.LabelSelector) > 0 {
		sel = labels.SelectorFromSet(filter.LabelSelector)
	}

	for _, oriVolume := range volumes {
		// ID filter check (if ID filtering needs to be combined with others)
		if filter.Id != "" && oriVolume.Metadata.Id != filter.Id {
			continue
		}

		// Label filter check
		if sel != nil && !sel.Matches(labels.Set(oriVolume.Metadata.Labels)) {
			continue
		}

		res = append(res, oriVolume)
	}
	return res
}

// listAndConvert is a helper to convert ceph images to ori volumes
func (s *Server) listAndConvert(ctx context.Context, log logr.Logger, cephImages []*cephapi.Image) ([]*ori.Volume, error) {
	res := make([]*ori.Volume, 0, len(cephImages)) // Pre-allocate slice
	for _, cephImage := range cephImages {
		if !apiutils.IsManagedBy(cephImage, apiutils.VolumeManager) {
			continue
		}

		oriVolume, err := s.convertImageToOriVolume(cephImage)
		if err != nil {
			// Log the specific image ID that failed conversion
			log.Error(err, "Failed to convert ceph image to ori volume", "imageID", cephImage.GetID())
			// Decide whether to skip this volume or fail the whole list
			// Let's skip for now to potentially return partial results
			continue
			// return nil, fmt.Errorf("error converting image %s: %w", cephImage.GetID(), err) // Fail fast option
		}
		res = append(res, oriVolume)
	}
	return res, nil
}

// listVolumes fetches all managed volumes (used when no specific filter is fast-tracked).
func (s *Server) listVolumes(ctx context.Context, log logr.Logger) ([]*ori.Volume, error) {
	cephImages, err := s.imageStore.List(ctx)
	if err != nil {
		// Log the underlying error from the store
		log.Error(err, "Error listing volumes from image store")
		return nil, fmt.Errorf("error listing volumes: %w", err)
	}
	log.V(1).Info("Listed all volumes from store", "count", len(cephImages))

	return s.listAndConvert(ctx, log, cephImages)
}

func (s *Server) ListVolumes(ctx context.Context, req *ori.ListVolumesRequest) (*ori.ListVolumesResponse, error) {
	log := s.loggerFrom(ctx)
	log.V(2).Info("Listing volumes")

	// --- Fast path for ID filter ---
	if filter != nil && filter.Id != "" {
		log.V(1).Info("Filtering by Volume ID", "VolumeID", filter.Id)
		volume, err := s.getOriVolume(ctx, log, filter.Id)
		if err != nil {
			// Handle NotFound specifically
			if status.Code(err) == codes.NotFound {
				log.V(1).Info("Volume not found by ID", "VolumeID", filter.Id)
				return &ori.ListVolumesResponse{Volumes: []*ori.Volume{}}, nil
			}
			// Log and return other errors
			log.Error(err, "Error getting volume by ID", "VolumeID", filter.Id)
			return nil, err // Return Internal error or similar
		}
		// Found by ID
		return &ori.ListVolumesResponse{Volumes: []*ori.Volume{volume}}, nil
	}

	// --- Fast path for LabelSelector filter (using the new index) ---
	if filter != nil && filter.Id == "" && len(filter.LabelSelector) > 0 {
		log.V(1).Info("Filtering by Label Selector using index", "Selector", filter.LabelSelector)

		// Assert that the store implements the required interface
		listerWithLabels, ok := s.imageStore.(imageListerWithLabels)
		if !ok {
			log.Error(fmt.Errorf("imageStore does not support ListByLabels"), "Cannot use label index optimization")
			// Fallback to slow path: list all and filter manually
			// Or return an error: return nil, status.Error(codes.Internal, "server configuration error: label index not supported")
			// Let's fallback for now, though it defeats the purpose.
			goto SlowPath
		}

		cephImages, err := listerWithLabels.ListByLabels(ctx, filter.LabelSelector)
		if err != nil {
			log.Error(err, "Error listing volumes by labels from store", "Selector", filter.LabelSelector)
			return nil, fmt.Errorf("error listing volumes by labels: %w", err)
		}
		log.V(1).Info("Listed volumes using label index", "count", len(cephImages))

		// Convert results
		volumes, err := s.listAndConvert(ctx, log, cephImages)
		if err != nil {
			// listAndConvert already logs errors per image
			log.Error(err, "Error converting volumes found by label", "Selector", filter.LabelSelector)
			// Return error as conversion failure might indicate bigger issues
			return nil, fmt.Errorf("error converting listed volumes: %w", err)
		}

		return &ori.ListVolumesResponse{Volumes: volumes}, nil
	}

	// --- Slow path: No specific filter or fallback ---
SlowPath:
	log.V(1).Info("Listing all volumes (no specific filter or fallback)")
	volumes, err := s.listVolumes(ctx, log) // Lists *all* managed volumes
	if err != nil {
		// listVolumes already logs the store error
		return nil, err // Return Internal or appropriate error
	}

	// Apply filter manually if needed (e.g., if fallback occurred or other filter types exist)
	// This part is now less critical if the label filter uses the index path.
	// volumes = s.filterVolumes(volumes, filter)
	// log.V(1).Info("Filtered volumes manually", "count", len(volumes))

	log.V(2).Info("Returning volumes list")
	return &ori.ListVolumesResponse{
		Volumes: volumes,
	}, nil
}
