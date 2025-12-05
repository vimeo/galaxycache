package minigc

import (
	"context"
	"sync"
	"time"

	"github.com/vimeo/galaxycache/lru"
	"github.com/vimeo/galaxycache/singleflight"
)

// ClusterHydrateInfo provides context around the returned value
type ClusterHydrateInfo struct {
	// Expiration is an absolute time after which this value is no longer
	// valid and should not be returned.
	// The zero-value means no-expiration.
	Expiration time.Time
}

type ClusterHydrateCB[K comparable, V any] func(ctx context.Context, key K) (V, ClusterHydrateInfo, error)

type flightGroupVal[V any] struct {
	val     V
	hydInfo ClusterHydrateInfo
}

// Cluster is an in-process cache intended for in-memory data
// It's a thin wrapper around another cache implementation that takes care of
// filling cache misses instead of requiring Add() calls.
// It leverages the singleflight package to handle cases where hydration can
// fail and/or block.
//
// In contrast, [StarSystem] is designed for cases where constructing/hydrating/fetching a
// value is quick, can't fail, but not completely free. (It's a lighter-weight implementation)
//
// Note: the underlying cache implementation may change at any time.
type Cluster[K comparable, V any] struct {
	lru       lru.TypedCache[K, V]
	hydrateCB ClusterHydrateCB[K, V]
	sfG       singleflight.TypedGroup[K, flightGroupVal[V]]
	mu        sync.Mutex
}

type ClusterParams[K comparable, V any] struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specificies a callback function to be
	// executed when an typedEntry is purged from the cache.
	OnEvicted func(key K, value V)

	MaxTTL time.Duration
}

func NewCluster[K comparable, V any](cb ClusterHydrateCB[K, V], params ClusterParams[K, V]) *Cluster[K, V] {
	return &Cluster[K, V]{
		hydrateCB: cb,
		lru: lru.TypedCache[K, V]{
			MaxEntries: params.MaxEntries,
			OnEvicted:  params.OnEvicted,
		},
	}
}

// Remove deletes the specified key from the cache
func (s *Cluster[K, V]) Remove(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lru.Remove(key)
}

func (s *Cluster[K, V]) get(key K) (V, ClusterHydrateInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, exp, ok := s.lru.GetWithExpiry(key); ok {
		return v, ClusterHydrateInfo{Expiration: exp}, true
	}
	var vz V
	return vz, ClusterHydrateInfo{}, false
}

// Get fetches the specified key from the cache, and calls the [ClusterHydrateCB] if not present.
// Overlapping calls to Get for the same key will be singleflighted.
func (s *Cluster[K, V]) Get(ctx context.Context, key K) (V, ClusterHydrateInfo, error) {
	// Before involving singleflight, do a quick check for the key to
	// reduce the cost of the uncontended case.
	if v, hydInfo, ok := s.get(key); ok {
		return v, hydInfo, nil
	}

	fgv, doErr := s.sfG.Do(key, func() (flightGroupVal[V], error) {
		if v, hydInfo, ok := s.get(key); ok {
			return flightGroupVal[V]{val: v, hydInfo: hydInfo}, nil
		}
		val, hydInfo, hydErr := s.hydrateCB(ctx, key)
		if hydErr != nil {
			return flightGroupVal[V]{}, hydErr
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.lru.AddExpiring(key, val, hydInfo.Expiration)
		return flightGroupVal[V]{val: val, hydInfo: hydInfo}, nil
	})
	return fgv.val, fgv.hydInfo, doErr
}
