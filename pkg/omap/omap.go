package omap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings" // Import strings package
	"sync"
	"time"

	"github.com/ceph/go-ceph/rados"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/onmetal/cephlet/ori/volume/apiutils"
	"github.com/onmetal/cephlet/pkg/api"
	"github.com/onmetal/cephlet/pkg/store"
	"github.com/onmetal/cephlet/pkg/utils"
)

// CreateStrategy remains the same
type CreateStrategy[E api.Object] interface {
	PrepareForCreate(obj E)
}

var ErrResourceVersionNotLatest = errors.New("resourceVersion is not latest")

// DefaultIteratorSize is a reasonable batch size for GetAllOmapValues internal iteration.
const DefaultIteratorSize int64 = 1024

// Options remains the same
type Options[E api.Object] struct {
	OmapName       string
	NewFunc        func() E
	CreateStrategy CreateStrategy[E]
}

// New function now accepts the RadosConnection interface
func New[E api.Object](conn RadosConnection, pool string, log logr.Logger, opts Options[E]) (*Store[E], error) {
	if conn == nil {
		return nil, fmt.Errorf("must specify conn")
	}
	if pool == "" {
		return nil, fmt.Errorf("must specify pool")
	}
	if opts.OmapName == "" {
		return nil, fmt.Errorf("must specify opts.OmapName")
	}
	if opts.NewFunc == nil {
		return nil, fmt.Errorf("must specify opts.NewFunc")
	}

	return &Store[E]{
		conn:     conn,
		pool:     pool,
		omapName: opts.OmapName,
		// Initialize cache map to store raw bytes
		cache: make(map[string][]byte),
		// Initialize the label index
		labelIndex: make(map[string]sets.Set[string]),
		watches:    sets.New[*watch[E]](),

		newFunc:        opts.NewFunc,
		createStrategy: opts.CreateStrategy,
		log:            log,
	}, nil
}

type Store[E api.Object] struct {
	conn     RadosConnection
	pool     string
	omapName string

	newFunc        func() E
	createStrategy CreateStrategy[E]

	// Cache related fields
	cacheMu     sync.RWMutex
	cache       map[string][]byte           // Cache now stores raw byte data
	labelIndex  map[string]sets.Set[string] // <--- Add label index field (labelKey=labelValue -> Set[objectID])
	initialized bool
	initMu      sync.Mutex
	initErr     error

	// Watch related fields
	watchesMu sync.RWMutex
	watches   sets.Set[*watch[E]]

	log logr.Logger
}

// --- Internal Label Index Helper ---

// formatLabel formats a key-value pair for the index.
func formatLabel(key, value string) string {
	// Using a simple separator. Ensure it doesn't clash with potential key/value content.
	// Consider base64 encoding key/value if they can contain '=' or the separator.
	return fmt.Sprintf("%s=%s", key, value)
}

// updateLabelIndex updates the index for a single object based on its labels.
// IMPORTANT: Call this method while holding the cacheMu write lock.
func (s *Store[E]) updateLabelIndex(objID string, oldLabels, newLabels map[string]string) {
	s.log.V(2).Info("Updating label index", "id", objID)
	oldLabelSet := sets.New[string]()
	for k, v := range oldLabels {
		oldLabelSet.Insert(formatLabel(k, v))
	}

	newLabelSet := sets.New[string]()
	for k, v := range newLabels {
		newLabelSet.Insert(formatLabel(k, v))
	}

	// Labels to remove objID from
	for label := range oldLabelSet.Difference(newLabelSet) {
		if ids, ok := s.labelIndex[label]; ok {
			ids.Delete(objID)
			if ids.Len() == 0 {
				delete(s.labelIndex, label) // Clean up empty sets
			}
		}
	}

	// Labels to add objID to
	for label := range newLabelSet.Difference(oldLabelSet) {
		if _, ok := s.labelIndex[label]; !ok {
			s.labelIndex[label] = sets.New[string]()
		}
		s.labelIndex[label].Insert(objID)
	}
	s.log.V(2).Info("Label index updated", "id", objID)
}

// removeFromLabelIndex removes an object entirely from the label index.
// IMPORTANT: Call this method while holding the cacheMu write lock.
func (s *Store[E]) removeFromLabelIndex(objID string, labels map[string]string) {
	s.log.V(2).Info("Removing object from label index", "id", objID)
	for k, v := range labels {
		label := formatLabel(k, v)
		if ids, ok := s.labelIndex[label]; ok {
			ids.Delete(objID)
			if ids.Len() == 0 {
				delete(s.labelIndex, label)
			}
		}
	}
	s.log.V(2).Info("Object removed from label index", "id", objID)
}

// getLabelsFromObject tries to extract labels using volume apiutils.
// Returns nil, nil if extraction fails.
func (s *Store[E]) getLabelsFromObject(obj api.Object) (map[string]string, error) {
	// We need api.Metadata to pass to GetLabelsAnnotation
	// This relies on the generic E implementing the api.Object interface correctly
	// We assume the relevant Annotations map is accessible via obj.GetAnnotations()
	metadata := api.Metadata{
		Annotations: obj.GetAnnotations(),
		// Other fields are not needed for GetLabelsAnnotation
	}
	labels, err := apiutils.GetLabelsAnnotation(metadata)
	if err != nil {
		// It's common for the annotation to be missing initially, treat as no labels
		// Log other errors but don't fail the whole operation
		if strings.Contains(err.Error(), "object has no labels at") {
			return nil, nil // No labels annotation present
		}
		s.log.Error(err, "Failed to get labels annotation from object, skipping label indexing for this object", "id", obj.GetID())
		return nil, err // Return the error for context, but allow caller to proceed
	}
	return labels, nil
}

// initializeCache populates the cache with raw byte data and builds the label index.
func (s *Store[E]) initializeCache(_ context.Context) error {
	s.initMu.Lock()
	// Early exit if already initialized
	if s.initialized {
		err := s.initErr // Capture error before unlocking
		s.initMu.Unlock()
		return err
	}
	// Defer unlock until the end of the function
	defer s.initMu.Unlock()

	s.log.V(1).Info("Initializing OMAP cache and label index")

	// Acquire write lock for cache and index modification
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	ioCtx, err := s.conn.OpenIOContext(s.pool)
	if err != nil {
		s.initErr = fmt.Errorf("failed to open IO context for cache initialization: %w", err)
		s.initialized = true // Mark as initialized even on error to prevent retries
		s.log.Error(s.initErr, "Cache initialization failed")
		return s.initErr
	}
	defer ioCtx.Destroy()

	// Fetch all OMAP values
	omapValues, err := ioCtx.GetAllOmapValues(s.omapName, "", "", DefaultIteratorSize)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			s.log.V(1).Info("OMAP not found during initial cache load, starting with empty cache and index.")
			s.initialized = true
			s.initErr = nil
			s.cache = make(map[string][]byte)                // Ensure cache is initialized
			s.labelIndex = make(map[string]sets.Set[string]) // Ensure index is initialized
			return nil
		}
		s.initErr = fmt.Errorf("failed to get all omap values for cache initialization: %w", err)
		s.initialized = true // Mark as initialized even on error
		s.log.Error(s.initErr, "Cache initialization failed")
		return s.initErr
	}

	// Reset cache and index before population
	s.cache = make(map[string][]byte, len(omapValues))
	s.labelIndex = make(map[string]sets.Set[string])

	// Populate cache and build label index
	for k, v := range omapValues {
		// Store raw bytes in cache
		s.cache[k] = v

		// Unmarshal *only* for label indexing
		obj := s.newFunc()
		if err := json.Unmarshal(v, &obj); err != nil {
			// Log the error but DO NOT return it. Allow initialization to continue.
			s.log.Error(err, "Failed to unmarshal object during cache init for label indexing, skipping index update", "id", k)
			continue // Skip indexing for this object
		}

		// Extract labels and update index
		labels, _ := s.getLabelsFromObject(obj) // Ignore error here, handled in getLabelsFromObject
		for labelKey, labelValue := range labels {
			label := formatLabel(labelKey, labelValue)
			if _, ok := s.labelIndex[label]; !ok {
				s.labelIndex[label] = sets.New[string]()
			}
			s.labelIndex[label].Insert(k) // k is the object ID
		}
	}

	s.initialized = true
	s.initErr = nil // Set initErr to nil only on successful completion
	s.log.V(1).Info("OMAP cache and label index initialized successfully", "cachedEntries", len(s.cache), "indexedLabels", len(s.labelIndex))
	return nil
}

// enqueue sends the *unmarshaled* object to watchers.
func (s *Store[E]) enqueue(evt store.WatchEvent[E]) {
	// Keep the RLock for reading watchers list
	s.watchesMu.RLock()
	watchers := s.watches.UnsortedList() // Get list under read lock
	s.watchesMu.RUnlock()                // Release lock before potentially blocking send

	for _, handler := range watchers { // Iterate over the local copy
		select {
		case handler.events <- evt:
		default:
			s.log.V(1).Info("Watch channel buffer full, dropping event", "event", evt)
			// Maybe remove the watcher if buffer is consistently full?
			// Consider adding metrics for dropped events.
		}
	}
}

// watchHandlers returns a snapshot of current watch handlers.
// Used internally by enqueue.
func (s *Store[E]) watchHandlers() []*watch[E] {
	s.watchesMu.RLock()
	defer s.watchesMu.RUnlock()
	// Return a copy to avoid holding the lock during event sending
	return s.watches.UnsortedList()
}

// --- Internal OMAP Helper Methods ---

// getSingleOmapValue retrieves only one value, maxReturn=1 is correct here.
func (s *Store[E]) getSingleOmapValue(ioCtx RadosIOContext, omapName, key string) ([]byte, error) {
	// Use maxReturn=1 because we only want *this specific key* if it exists.
	// This is different from GetAllOmapValues where the parameter is iteratorSize.
	omap, err := ioCtx.GetAllOmapValues(omapName, "", key, 1) // Fetch starting from key, limit 1
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return nil, rados.ErrNotFound // OMAP itself not found
		}
		return nil, err // Other Rados error
	}
	value, ok := omap[key]
	if !ok {
		// Key wasn't found in the OMAP (even if the OMAP exists)
		return nil, rados.ErrNotFound
	}
	return value, nil
}

func (s *Store[E]) deleteOmapValue(ioCtx RadosIOContext, omapName, key string) error {
	if err := ioCtx.RmOmapKeys(omapName, []string{key}); err != nil {
		// Don't wrap ErrNotFound, let caller handle it
		if errors.Is(err, rados.ErrNotFound) {
			return err
		}
		return fmt.Errorf("unable to delete omap value for key %q: %w", key, err)
	}
	return nil
}

func (s *Store[E]) setOmapValue(ioCtx RadosIOContext, omapName, key string, value []byte) error {
	if err := ioCtx.SetOmap(omapName, map[string][]byte{key: value}); err != nil {
		return fmt.Errorf("unable to set omap value for key %q: %w", key, err)
	}
	return nil
}

// --- Internal OMAP Operation Wrappers ---

// set performs the OMAP write operation.
func (s *Store[E]) set(ioCtx RadosIOContext, obj E) ([]byte, error) { // Returns marshaled data
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal obj %q: %w", obj.GetID(), err)
	}
	if err := s.setOmapValue(ioCtx, s.omapName, obj.GetID(), data); err != nil {
		return nil, fmt.Errorf("failed to put object mapping for %q: %w", obj.GetID(), err)
	}
	return data, nil // Return the marshaled data for cache update
}

// getFromOmap retrieves a single object directly from OMAP (used internally before writes).
func (s *Store[E]) getFromOmap(ioCtx RadosIOContext, id string) (E, []byte, error) { // Also returns raw data
	s.log.V(2).Info("Directly loading object from omap", "id", id)
	rawData, err := s.getSingleOmapValue(ioCtx, s.omapName, id) // Uses maxReturn=1 correctly
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			return utils.Zero[E](), nil, fmt.Errorf("object with id %q %w", id, store.ErrNotFound)
		}
		return utils.Zero[E](), nil, fmt.Errorf("failed to fetch omap value for id %q: %w", id, err)
	}
	obj := s.newFunc()
	if err := json.Unmarshal(rawData, &obj); err != nil {
		// This unmarshal error during internal check is different from Get/List
		return utils.Zero[E](), rawData, fmt.Errorf("failed to unmarshal object data for id %q from omap during internal check: %w", id, err)
	}
	return obj, rawData, nil
}

// deleteFromOmap performs the physical OMAP delete operation.
func (s *Store[E]) deleteFromOmap(ioCtx RadosIOContext, id string) error {
	if err := s.deleteOmapValue(ioCtx, s.omapName, id); err != nil {
		// Return ErrNotFound directly if that's the underlying cause
		if errors.Is(err, rados.ErrNotFound) {
			s.log.V(1).Info("Attempted to delete non-existent key from omap", "id", id)
			return store.ErrNotFound // Map rados not found to store not found
		}
		return fmt.Errorf("failed to delete object %q from omap: %w", id, err)
	}
	return nil
}

// --- Public Methods ---

func (s *Store[E]) Create(ctx context.Context, obj E) (E, error) {
	if err := s.initializeCache(ctx); err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to initialize cache for Create: %w", err)
	}

	objID := obj.GetID()
	s.log.V(1).Info("Creating object", "id", objID)

	ioCtx, err := s.conn.OpenIOContext(s.pool)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("unable to get io context for create check: %w", err)
	}
	defer ioCtx.Destroy()

	// Check OMAP directly for existence before create
	_, _, err = s.getFromOmap(ioCtx, objID) // Ignore obj and rawData
	switch {
	case err == nil:
		return utils.Zero[E](), fmt.Errorf("object with id %q %w", objID, store.ErrAlreadyExists)
	case errors.Is(err, store.ErrNotFound):
		break // OK to proceed
	default:
		// Handle potential unmarshal error from getFromOmap check
		if strings.Contains(err.Error(), "failed to unmarshal") {
			s.log.Error(err, "Corrupted data found in OMAP during create check, proceeding cautiously", "id", objID)
			// Decide if you want to overwrite corrupted data or fail here
			// For now, let's proceed, Create will overwrite.
		} else {
			return utils.Zero[E](), fmt.Errorf("failed to check object existence in omap for id %q: %w", objID, err)
		}
	}

	// Prepare and write to OMAP
	if s.createStrategy != nil {
		s.createStrategy.PrepareForCreate(obj)
	}
	obj.SetCreatedAt(time.Now().UTC().Truncate(time.Microsecond)) // Truncate for consistency
	obj.IncrementResourceVersion()                                // Increment to 1

	// 'set' now also returns the marshaled data
	rawData, err := s.set(ioCtx, obj)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to set object in omap: %w", err)
	}

	// Get labels *after* potential modification by strategy and before cache update
	currentLabels, _ := s.getLabelsFromObject(obj)

	// Update cache and index under lock
	s.cacheMu.Lock()
	if !s.initialized {
		s.cacheMu.Unlock()
		s.log.Error(errors.New("cache became uninitialized during Create"), "Cache state inconsistent")
		// Attempt to clean up OMAP entry? Risky. Best to signal error.
		_ = s.deleteFromOmap(ioCtx, objID) // Best effort cleanup
		return utils.Zero[E](), errors.New("cache consistency error during create")
	}
	s.cache[objID] = rawData                      // Store raw bytes
	s.updateLabelIndex(objID, nil, currentLabels) // Add to label index (no old labels)
	s.cacheMu.Unlock()

	// Enqueue watch event with the *unmarshaled* object
	s.enqueue(store.WatchEvent[E]{
		Type:   store.WatchEventTypeCreated,
		Object: obj, // Send the object state *after* strategy and version increment
	})

	s.log.V(1).Info("Object created successfully", "id", objID, "resourceVersion", obj.GetResourceVersion())
	return obj, nil
}

func (s *Store[E]) Delete(ctx context.Context, id string) error {
	if err := s.initializeCache(ctx); err != nil {
		// Allow delete to proceed even if cache init failed?
		// Might lead to inconsistencies if OMAP operation succeeds but cache isn't updated.
		// Let's return the error for now.
		return fmt.Errorf("failed to initialize cache for Delete: %w", err)
	}

	s.log.V(1).Info("Deleting object", "id", id)

	ioCtx, err := s.conn.OpenIOContext(s.pool)
	if err != nil {
		return fmt.Errorf("unable to get io context for delete: %w", err)
	}
	defer ioCtx.Destroy()

	// Get current state from OMAP to check finalizers and get labels
	// We need the object state *before* potential modification/deletion
	obj, rawDataBeforeDelete, err := s.getFromOmap(ioCtx, id) // Also get raw data
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.log.V(1).Info("Object not found in OMAP, ensuring removal from cache and index", "id", id)
			// Attempt to clean up cache/index just in case
			s.cacheMu.Lock()
			if oldRawData, exists := s.cache[id]; exists {
				// Unmarshal old data to get labels for index removal
				oldObjForLabels := s.newFunc()
				// Ignore unmarshal error here, best effort removal
				_ = json.Unmarshal(oldRawData, &oldObjForLabels)
				oldLabels, _ := s.getLabelsFromObject(oldObjForLabels)
				s.removeFromLabelIndex(id, oldLabels)
				delete(s.cache, id)
			}
			s.cacheMu.Unlock()
			return nil // Idempotent delete
		}
		// Handle potential unmarshal error from getFromOmap
		if strings.Contains(err.Error(), "failed to unmarshal") {
			s.log.Error(err, "Corrupted data found in OMAP during delete check, attempting physical delete", "id", id)
			// Proceed to physical delete attempt
		} else {
			return fmt.Errorf("failed to get object from omap for delete check: %w", err)
		}
		// If we fell through due to unmarshal error, obj is zero, proceed to physical delete
	}

	// Get labels *before* potential deletion for index cleanup
	labelsBeforeDelete, _ := s.getLabelsFromObject(obj) // Use the object fetched before delete attempt

	// Soft delete if finalizers exist and object wasn't corrupted
	if obj.GetID() != "" && len(obj.GetFinalizers()) > 0 { // Check obj.ID != "" for corrupted case
		if obj.GetDeletedAt() != nil {
			s.log.V(1).Info("Object already marked for deletion", "id", id)
			// Ensure cache reflects OMAP state if getFromOmap succeeded
			s.cacheMu.Lock()
			s.cache[id] = rawDataBeforeDelete // Update cache with potentially fetched data
			s.cacheMu.Unlock()
			return nil // No change needed
		}

		s.log.V(1).Info("Marking object for deletion (soft delete)", "id", id)
		now := time.Now().UTC().Truncate(time.Microsecond)
		obj.SetDeletedAt(&now)
		obj.IncrementResourceVersion()

		// Write updated object (with DeletedAt) to OMAP
		rawDataAfterSoftDelete, err := s.set(ioCtx, obj) // Get raw data back
		if err != nil {
			return fmt.Errorf("failed to set object metadata for soft deletion in omap: %w", err)
		}

		// Update cache and index (no change in labels, just update content)
		s.cacheMu.Lock()
		s.cache[id] = rawDataAfterSoftDelete
		// No index update needed as labels didn't change
		s.cacheMu.Unlock()

		// Enqueue watch event with the *unmarshaled* object *after* soft delete
		s.enqueue(store.WatchEvent[E]{
			Type:   store.WatchEventTypeDeleted, // Still a delete event type
			Object: obj,
		})
		s.log.V(1).Info("Object marked for deletion (soft delete) completed", "id", id, "resourceVersion", obj.GetResourceVersion())
		return nil
	}

	// Physical delete (no finalizers or corrupted data)
	s.log.V(1).Info("Physically deleting object", "id", id)
	if err := s.deleteFromOmap(ioCtx, id); err != nil {
		// Don't return error if OMAP delete failed because key was already gone
		if errors.Is(err, store.ErrNotFound) { // Check against store.ErrNotFound now
			s.log.V(1).Info("Object already physically deleted from OMAP", "id", id)
		} else {
			return fmt.Errorf("failed to physically delete object from omap: %w", err)
		}
	}

	// Remove from cache and label index
	s.cacheMu.Lock()
	delete(s.cache, id)
	s.removeFromLabelIndex(id, labelsBeforeDelete) // Use labels from before deletion
	s.cacheMu.Unlock()

	// Enqueue physical delete event - send the state *before* deletion
	s.enqueue(store.WatchEvent[E]{
		Type:   store.WatchEventTypeDeleted,
		Object: obj, // Send the object state as it was before physical delete
	})

	s.log.V(1).Info("Object physically deleted successfully", "id", id)
	return nil
}

func (s *Store[E]) Get(ctx context.Context, id string) (E, error) {
	if err := s.initializeCache(ctx); err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to initialize cache for Get: %w", err)
	}

	s.log.V(2).Info("Getting object from cache", "id", id)
	s.cacheMu.RLock()
	rawData, found := s.cache[id]
	s.cacheMu.RUnlock() // Unlock after reading raw data

	if !found {
		// We are always relying on the cache and are not trying to pull through.
		s.log.V(1).Info("Object not found in cache", "id", id)
		return utils.Zero[E](), fmt.Errorf("object with id %q %w", id, store.ErrNotFound)
	}

	// Unmarshal the raw data retrieved from cache
	obj := s.newFunc()
	if err := json.Unmarshal(rawData, &obj); err != nil {
		s.log.Error(err, "Failed to unmarshal object data from cache", "id", id)
		// Consider removing corrupted data from cache?
		// s.cacheMu.Lock()
		// delete(s.cache, id)
		// s.cacheMu.Unlock()
		return utils.Zero[E](), fmt.Errorf("failed to unmarshal object data for id %q: %w", id, err)
	}

	// Get should still return objects marked for deletion if they haven't been physically removed yet.
	s.log.V(2).Info("Object found and unmarshaled from cache", "id", id)
	return obj, nil
}

func (s *Store[E]) Update(ctx context.Context, obj E) (E, error) {
	if err := s.initializeCache(ctx); err != nil {
		return utils.Zero[E](), fmt.Errorf("failed to initialize cache for Update: %w", err)
	}

	objID := obj.GetID()
	s.log.V(1).Info("Updating object", "id", objID, "inputResourceVersion", obj.GetResourceVersion())

	ioCtx, err := s.conn.OpenIOContext(s.pool)
	if err != nil {
		return utils.Zero[E](), fmt.Errorf("unable to get io context for update: %w", err)
	}
	defer ioCtx.Destroy()

	// Check existence and get old version from OMAP for ResourceVersion check
	oldObj, _, err := s.getFromOmap(ioCtx, objID) // Ignore raw data for now
	if err != nil {
		// Handle ErrNotFound specifically
		if errors.Is(err, store.ErrNotFound) {
			s.log.V(1).Info("Object not found in OMAP during update check", "id", objID)
			// Clean up cache just in case
			s.cacheMu.Lock()
			delete(s.cache, objID) // Remove if it somehow exists in cache
			// Assuming labels were not found, no index update needed
			s.cacheMu.Unlock()
			return utils.Zero[E](), err // Return the ErrNotFound
		}
		// Handle potential unmarshal error
		if strings.Contains(err.Error(), "failed to unmarshal") {
			s.log.Error(err, "Corrupted data found in OMAP during update check", "id", objID)
			// Fail the update if existing data is corrupted? Or allow overwrite?
			// Let's fail for safety.
			return utils.Zero[E](), fmt.Errorf("cannot update object %q due to existing corrupted data in OMAP: %w", objID, err)
		}
		// Other errors during getFromOmap
		return utils.Zero[E](), fmt.Errorf("failed to get existing object from omap for update check: %w", err)
	}

	// Get labels from the old and new state *before* OMAP/cache updates
	oldLabels, _ := s.getLabelsFromObject(oldObj)
	newLabels, _ := s.getLabelsFromObject(obj)

	// --- Begin OMAP and Cache Update Logic ---
	deleted := false
	var rawDataAfterUpdate []byte // To store marshaled data for cache

	// Physical delete case (triggered by update)
	if obj.GetDeletedAt() != nil && len(obj.GetFinalizers()) == 0 {
		s.log.V(1).Info("Update triggers physical deletion", "id", objID)
		// Optimistic concurrency check *before* deleting OMAP entry
		if oldObj.GetResourceVersion() != obj.GetResourceVersion() {
			s.log.V(1).Info("ResourceVersion mismatch during update-triggered delete", "id", objID, "expected", oldObj.GetResourceVersion(), "got", obj.GetResourceVersion())
			return utils.Zero[E](), fmt.Errorf("failed to delete object during update: %w", ErrResourceVersionNotLatest)
		}

		if err := s.deleteFromOmap(ioCtx, objID); err != nil {
			if !errors.Is(err, store.ErrNotFound) { // Check store.ErrNotFound
				return utils.Zero[E](), fmt.Errorf("failed to delete object from omap during update: %w", err)
			}
			s.log.V(1).Info("Object already physically deleted from OMAP during update", "id", objID)
		}
		deleted = true
		// Keep obj as it was passed in for the event

	} else { // Standard Update case
		s.log.V(1).Info("Performing standard update", "id", objID)
		// Optimistic concurrency check using ResourceVersion
		// theoretical side case if omap is changed outside the cephlet
		if oldObj.GetResourceVersion() != obj.GetResourceVersion() {
			s.log.V(1).Info("ResourceVersion mismatch during update", "id", objID, "expected", oldObj.GetResourceVersion(), "got", obj.GetResourceVersion())
			return utils.Zero[E](), fmt.Errorf("failed to update object: %w", ErrResourceVersionNotLatest)
		}
		obj.IncrementResourceVersion() // Increment version before saving

		// Set/update the object in OMAP and get raw data
		rawDataAfterUpdate, err = s.set(ioCtx, obj)
		if err != nil {
			// Revert version increment on failure? Maybe not, let caller retry.
			return utils.Zero[E](), fmt.Errorf("failed to set object in omap during update: %w", err)
		}
		s.log.V(1).Info("Object updated in OMAP", "id", objID, "newResourceVersion", obj.GetResourceVersion())
	}

	// --- Update Cache and Index, Enqueue Event ---
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	if !s.initialized {
		s.log.Error(errors.New("cache became uninitialized during Update"), "Cache state inconsistent")
		// Attempt cleanup if deletion happened? Complex. Error out.
		return utils.Zero[E](), errors.New("cache consistency error during update")
	}

	var eventType store.WatchEventType
	if deleted {
		delete(s.cache, objID)
		s.removeFromLabelIndex(objID, oldLabels) // Use old labels for removal
		eventType = store.WatchEventTypeDeleted
		s.log.V(1).Info("Object removed from cache and index", "id", objID)
	} else {
		s.cache[objID] = rawDataAfterUpdate             // Update cache with new raw bytes
		s.updateLabelIndex(objID, oldLabels, newLabels) // Update index based on label changes
		eventType = store.WatchEventTypeUpdated
		s.log.V(1).Info("Object updated in cache and index", "id", objID)
	}

	// Enqueue event with the *unmarshaled* object state *after* potential version increment
	s.enqueue(store.WatchEvent[E]{
		Type:   eventType,
		Object: obj,
	})

	s.log.V(1).Info("Update operation completed", "id", objID, "deleted", deleted, "finalResourceVersion", obj.GetResourceVersion())
	return obj, nil
}

// watch and related types remain the same
type watch[E api.Object] struct {
	store  *Store[E]
	events chan store.WatchEvent[E]
}

func (w *watch[E]) Stop() {
	w.store.watchesMu.Lock()
	defer w.store.watchesMu.Unlock()

	// Check if the watch exists before trying to delete and close
	if w.store.watches.Has(w) {
		w.store.watches.Delete(w)
		close(w.events) // Close the channel to signal consumers
		w.store.log.V(1).Info("Stopped and closed watch channel")
	} else {
		// This can happen if Stop is called multiple times or after the store removed it.
		w.store.log.V(1).Info("Attempted to stop a watch that was already stopped or removed")
	}
}

func (w *watch[E]) Events() <-chan store.WatchEvent[E] {
	return w.events
}

func (s *Store[E]) Watch(ctx context.Context) (store.Watch[E], error) {
	if err := s.initializeCache(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize cache for Watch: %w", err)
	}

	// Potentially check ctx.Err() here?
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled or timed out before Watch creation: %w", err)
	}

	s.watchesMu.Lock()
	defer s.watchesMu.Unlock()

	w := &watch[E]{
		store: s,
		// Buffer size could be configurable
		events: make(chan store.WatchEvent[E], 100),
	}

	s.watches.Insert(w)
	s.log.V(1).Info("Watch created")

	// Optional: Send initial state? store.Watch usually only sends changes.
	// If needed, iterate over cache and send Create events here (under lock).

	// Optional: Goroutine to remove watch if context passed to Watch() is cancelled?
	// go func() {
	//   <-ctx.Done()
	//   s.log.V(1).Info("Context cancelled, stopping watch", "reason", ctx.Err())
	//   w.Stop()
	// }()

	return w, nil
}

func (s *Store[E]) List(ctx context.Context) ([]E, error) {
	if err := s.initializeCache(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize cache for List: %w", err)
	}

	s.log.V(2).Info("Listing objects from cache")
	s.cacheMu.RLock()
	// Create a snapshot of the cache values to iterate over after releasing the lock
	rawDataList := make([][]byte, 0, len(s.cache))
	idsForDebug := make([]string, 0, len(s.cache)) // For logging potential errors
	for id, data := range s.cache {
		rawDataList = append(rawDataList, data)
		idsForDebug = append(idsForDebug, id) // Collect IDs corresponding to rawDataList order
	}
	s.cacheMu.RUnlock() // Release lock before unmarshaling potentially many items

	objs := make([]E, 0, len(rawDataList))
	for i, rawData := range rawDataList {
		obj := s.newFunc()
		if err := json.Unmarshal(rawData, &obj); err != nil {
			objID := idsForDebug[i] // Get corresponding ID
			s.log.Error(err, "Failed to unmarshal object data from cache during List", "id", objID)
			firstUnmarshalError := fmt.Errorf("failed to unmarshal object data for id %q during list: %w", objID, err)
			// Fail fast: return immediately on the first error
			return nil, firstUnmarshalError
		}

		objs = append(objs, obj)
	}

	// If loop completes without error
	s.log.V(2).Info("Returning objects from cache", "count", len(objs))
	return objs, nil
}

// ListByLabels retrieves objects matching the given label selector using the index.
func (s *Store[E]) ListByLabels(ctx context.Context, labelSelector map[string]string) ([]E, error) {
	// Ensure the cache and index are ready before proceeding.
	if err := s.initializeCache(ctx); err != nil {
		// Wrap the initialization error for better context.
		return nil, fmt.Errorf("failed to initialize cache for ListByLabels: %w", err)
	}

	// If the selector is empty, no filtering is needed, return all items.
	if len(labelSelector) == 0 {
		s.log.V(1).Info("Empty label selector provided, returning all items (like List)")
		// Delegate to the List method which handles fetching all items.
		return s.List(ctx)
	}

	s.log.V(1).Info("Listing objects by labels from index", "selector", labelSelector)
	s.cacheMu.RLock() // Read lock is sufficient for accessing labelIndex and cache.
	defer s.cacheMu.RUnlock()

	// Initialize intersection set to nil. It will be populated with the IDs
	// matching the *first* label encountered in the selector.
	var intersection sets.Set[string] // Use nil to indicate it hasn't been initialized yet.

	// Iterate through the label selector to find matching object IDs.
	for key, value := range labelSelector {
		label := formatLabel(key, value)
		ids, found := s.labelIndex[label]

		// If *any* label in the selector doesn't exist in the index,
		// then no objects can possibly match the *entire* selector.
		if !found {
			s.log.V(1).Info("Label not found in index, no objects match the full selector", "label", label, "selector", labelSelector)
			return []E{}, nil // Return empty list immediately.
		}

		// If this is the first label we've processed, initialize the intersection set.
		if intersection == nil {
			// Clone the set from the index to avoid modifying the index data.
			intersection = ids.Clone()
			s.log.V(1).Info("Initialized intersection set", "label", label, "initial_count", intersection.Len())
		} else {
			// For subsequent labels, compute the intersection with the current set.
			// The Intersection method modifies the receiver set (`intersection`).
			prevCount := intersection.Len()
			intersection = intersection.Intersection(ids) // Modifies 'intersection' in place
			s.log.V(1).Info("Computed intersection", "label", label, "previous_count", prevCount, "new_count", intersection.Len())
		}

		// Optimization: If the intersection becomes empty at any point,
		// no objects can match the full selector, so we can exit early.
		if intersection.Len() == 0 {
			s.log.V(1).Info("Intersection of label matches became empty, returning empty list", "selector", labelSelector)
			return []E{}, nil
		}
	}

	// If the loop finishes, but intersection is still nil (shouldn't happen if len(labelSelector)>0
	// and we didn't return early) or is empty, it means no objects matched.
	// This check primarily covers the case where the intersection became empty on the last iteration.
	if intersection == nil || intersection.Len() == 0 {
		s.log.V(1).Info("No matching objects found for the given label selector after checking all labels", "selector", labelSelector)
		return []E{}, nil
	}

	// Retrieve and unmarshal objects corresponding to the IDs remaining in the intersection set.
	// Pre-allocate slice capacity for efficiency.
	objs := make([]E, 0, intersection.Len())
	// Removed: var firstUnmarshalError error // No longer needed for immediate fail-fast

	for id := range intersection {
		rawData, found := s.cache[id]
		// This check guards against potential inconsistencies between the labelIndex and the main cache.
		if !found {
			// Log an error, as this indicates a potential bug or race condition in cache updates.
			err := fmt.Errorf("object ID %q found in label index but not in cache during ListByLabels", id)
			s.log.Error(err, "Cache inconsistency detected")
			// Returning an error might be safer if consistency is paramount.
			// Let's capture it as the primary error and stop processing.
			return nil, err // Fail fast on inconsistency
		}

		// Create a new object of type E using the provided factory function.
		obj := s.newFunc()
		// Unmarshal the raw data from the cache into the new object.
		if err := json.Unmarshal(rawData, &obj); err != nil {
			s.log.Error(err, "Failed to unmarshal object data from cache during ListByLabels", "id", id)
			// Directly create and return the specific error for fail-fast behavior.
			unmarshalErr := fmt.Errorf("failed to unmarshal object data for id %q during ListByLabels: %w", id, err)
			return nil, unmarshalErr // Fail fast on the first unmarshal error.
		}

		// Append the successfully unmarshalled object to the results.
		objs = append(objs, obj)
	}

	// No need for the final error check here as we fail fast earlier.

	s.log.V(1).Info("Successfully retrieved objects matching label selector", "count", len(objs), "selector", labelSelector)
	// Return the slice of matching objects.
	return objs, nil
}
