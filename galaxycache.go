/*
Copyright 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package galaxycache provides a data loading mechanism with caching
// and de-duplication that works across a set of peer processes.
//
// Each data Get first consults its local cache, otherwise delegates
// to the requested key's canonical owner, which then checks its cache
// or finally gets the data.  In the common case, many concurrent
// cache misses across a set of peers for the same key result in just
// one cache fill.
package galaxycache

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/vimeo/groupcache/groupcachepb"
	"github.com/vimeo/groupcache/lru"
	"github.com/vimeo/groupcache/singleflight"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/trace"
)

// A BackendGetter loads data for a key.
type BackendGetter interface {
	// Get returns the value identified by key, populating dest.
	//
	// The returned data must be unversioned. That is, key must
	// uniquely describe the loaded data, without an implicit
	// current time, and without relying on cache expiration
	// mechanisms.
	Get(ctx context.Context, key string, dest Sink) error
}

// A GetterFunc implements BackendGetter with a function.
type GetterFunc func(ctx context.Context, key string, dest Sink) error

// Get here calls the chosen Getter when the current star authority is the owner of a key, getting the value from the database, for example
func (f GetterFunc) Get(ctx context.Context, key string, dest Sink) error {
	return f(ctx, key, dest)
}

// Universe defines the primary container for all galaxycache operations. It contains the galaxies and StarAuthorityPicker
type Universe struct {
	mu                  sync.RWMutex
	galaxies            map[string]*Galaxy
	starAuthorityPicker *StarAuthorityPicker
}

// NewUniverse is the default constructor for the Universe object. It is passed a FetchProtocol (to specify fetching via GRPC or HTTP) and its own URL
func NewUniverse(protocol FetchProtocol, selfURL string) *Universe {
	return NewUniverseWithOpts(protocol, selfURL, nil)
}

// NewUniverseWithOpts is the optional constructor for the Universe object that defines a non-default hash function and number of replicas
func NewUniverseWithOpts(protocol FetchProtocol, selfURL string, options *HashOptions) *Universe {
	c := &Universe{
		galaxies:            make(map[string]*Galaxy),
		starAuthorityPicker: newStarAuthorityPicker(protocol, selfURL, options),
	}

	return c
}

// GetGalaxy returns the named galaxy previously created with NewGalaxy, or
// nil if there's no such galaxy.
func (universe *Universe) GetGalaxy(name string) *Galaxy {
	universe.mu.RLock()
	defer universe.mu.RUnlock()
	return universe.galaxies[name]
}

// NewGalaxy creates a coordinated galaxy-aware Getter from a Getter.
//
// The returned Getter tries (but does not guarantee) to run only one
// Get call at once for a given key across an entire set of star authority
// processes. Concurrent callers both in the local process and in
// other processes receive copies of the answer once the original Get
// completes.
//
// The galaxy name must be unique for each BackendGetter.
func (universe *Universe) NewGalaxy(name string, cacheBytes int64, getter BackendGetter) *Galaxy {
	if getter == nil {
		panic("nil Getter")
	}
	universe.mu.Lock()
	defer universe.mu.Unlock()

	if _, dup := universe.galaxies[name]; dup {
		panic("duplicate registration of galaxy " + name)
	}
	g := &Galaxy{
		name:                name,
		getter:              getter,
		starAuthorityPicker: universe.starAuthorityPicker,
		cacheBytes:          cacheBytes,
		loadGroup:           &singleflight.Group{},
	}
	universe.galaxies[name] = g
	return g
}

// Set updates the Universe's list of star authorities (contained in the StarAuthorityPicker).
// Each StarAuthorityURL value should be a valid base URL,
// for example "http://example.net:8000".
func (universe *Universe) Set(StarAuthorityURLs ...string) {
	universe.starAuthorityPicker.set(StarAuthorityURLs...)
}

// A Galaxy is a cache namespace and associated data loaded spread over
// a group of 1 or more machines.
type Galaxy struct {
	name                string
	getter              BackendGetter
	starAuthoritiesOnce sync.Once
	starAuthorityPicker *StarAuthorityPicker
	cacheBytes          int64 // limit for sum of mainCache and hotCache size

	// mainCache is a cache of the keys for which this process
	// (amongst the other star authorities) is authoritative. That is, this cache
	// contains keys which consistent hash on to this process's
	// star authority number.
	mainCache cache

	// hotCache contains keys/values for which this star authority is not
	// authoritative (otherwise they would be in mainCache), but
	// are popular enough to warrant mirroring in this process to
	// avoid going over the network to fetch from a star authority.  Having
	// a hotCache avoids network hotspotting, where a star authority's
	// network card could become the bottleneck on a popular key.
	// This cache is used sparingly to maximize the total number
	// of key/value pairs that can be stored globally.
	hotCache cache

	// loadGroup ensures that each key is only fetched once
	// (either locally or remotely), regardless of the number of
	// concurrent callers.
	loadGroup flightGroup

	_ int32 // force Stats to be 8-byte aligned on 32-bit platforms

	// Stats are statistics on the galaxy.
	Stats Stats
}

// flightGroup is defined as an interface which flightgroup.Group
// satisfies.  We define this so that we may test with an alternate
// implementation.
type flightGroup interface {
	// Done is called when Do is done.
	Do(key string, fn func() (interface{}, error)) (interface{}, error)
}

// Stats are per-galaxy statistics.
type Stats struct {
	Gets                AtomicInt // any Get request, including from star authorities
	CacheHits           AtomicInt // either cache was good
	StarAuthorityLoads  AtomicInt // either remote load or remote cache hit (not an error)
	StarAuthorityErrors AtomicInt
	Loads               AtomicInt // (gets - cacheHits)
	LoadsDeduped        AtomicInt // after singleflight
	LocalLoads          AtomicInt // total good local loads
	LocalLoadErrs       AtomicInt // total bad local loads
	ServerRequests      AtomicInt // gets that came over the network from star authorities
}

// Name returns the name of the galaxy.
func (g *Galaxy) Name() string {
	return g.name
}

// Get as defined here is the primary "get" called on a galaxy to find the value for the given key. It will first try the local cache, then on a cache miss it will search for which star authority is the owner of the key based on the consistent hash, then try either fetching remotely or getting with the Getter (such as from a database) if the calling Universe instance is the owner
func (g *Galaxy) Get(ctx context.Context, key string, dest Sink) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ctx, _ = tag.New(ctx, tag.Insert(keyCommand, "get"))

	ctx, span := trace.StartSpan(ctx, "golang.org/groupcache.(*Galaxy).Get")
	startTime := time.Now()
	defer func() {
		stats.Record(ctx, MRoundtripLatencyMilliseconds.M(sinceInMilliseconds(startTime)))
		span.End()
	}()

	// TODO(@odeke-em): Remove .Stats
	g.Stats.Gets.Add(1)
	stats.Record(ctx, MGets.M(1))
	if dest == nil {
		span.SetStatus(trace.Status{Code: trace.StatusCodeInvalidArgument, Message: "nil dest sink"})
		return errors.New("galaxycache: nil dest Sink")
	}
	value, cacheHit := g.lookupCache(key)
	stats.Record(ctx, MKeyLength.M(int64(len(key))))

	if cacheHit {
		span.Annotatef(nil, "Cache hit")
		// TODO(@odeke-em): Remove .Stats
		g.Stats.CacheHits.Add(1)
		stats.Record(ctx, MCacheHits.M(1), MValueLength.M(int64(value.Len())))
		return setSinkView(dest, value)
	}

	stats.Record(ctx, MCacheMisses.M(1))
	span.Annotatef(nil, "Cache miss")
	// Optimization to avoid double unmarshalling or copying: keep
	// track of whether the dest was already populated. One caller
	// (if local) will set this; the losers will not. The common
	// case will likely be one caller.
	destPopulated := false
	value, destPopulated, err := g.load(ctx, key, dest)
	if err != nil {
		span.SetStatus(trace.Status{Code: trace.StatusCodeUnknown, Message: "nil dest sink"})
		stats.Record(ctx, MLoadErrors.M(1))
		return err
	}
	stats.Record(ctx, MValueLength.M(int64(value.Len())))
	if destPopulated {
		return nil
	}
	return setSinkView(dest, value)
}

// load loads key either by invoking the getter locally or by sending it to another machine.
func (g *Galaxy) load(ctx context.Context, key string, dest Sink) (value ByteView, destPopulated bool, err error) {
	// TODO(@odeke-em): Remove .Stats
	g.Stats.Loads.Add(1)
	stats.Record(ctx, MLoads.M(1))

	viewi, err := g.loadGroup.Do(key, func() (interface{}, error) {
		// Check the cache again because singleflight can only dedup calls
		// that overlap concurrently.  It's possible for 2 concurrent
		// requests to miss the cache, resulting in 2 load() calls.  An
		// unfortunate goroutine scheduling would result in this callback
		// being run twice, serially.  If we don't check the cache again,
		// cache.nbytes would be incremented below even though there will
		// be only one entry for this key.
		//
		// Consider the following serialized event ordering for two
		// goroutines in which this callback gets called twice for the
		// same key:
		// 1: Get("key")
		// 2: Get("key")
		// 1: lookupCache("key")
		// 2: lookupCache("key")
		// 1: load("key")
		// 2: load("key")
		// 1: loadGroup.Do("key", fn)
		// 1: fn()
		// 2: loadGroup.Do("key", fn)
		// 2: fn()
		if value, cacheHit := g.lookupCache(key); cacheHit {
			// TODO(@odeke-em): Remove .Stats
			g.Stats.CacheHits.Add(1)
			stats.Record(ctx, MCacheHits.M(1), MLocalLoads.M(1))
			return value, nil
		}
		// TODO(@odeke-em): Remove .Stats
		g.Stats.LoadsDeduped.Add(1)
		stats.Record(ctx, MLoadsDeduped.M(1))

		var value ByteView
		var err error
		if starAuth, ok := g.starAuthorityPicker.pickStarAuthority(key); ok {
			value, err = g.getFromStarAuthority(ctx, starAuth, key)
			if err == nil {
				// TODO(@odeke-em): Remove .Stats
				g.Stats.StarAuthorityLoads.Add(1)
				stats.Record(ctx, MStarAuthorityLoads.M(1))
				return value, nil
			}
			// TODO(@odeke-em): Remove .Stats
			g.Stats.StarAuthorityErrors.Add(1)
			stats.Record(ctx, MStarAuthorityErrors.M(1))
			// TODO(bradfitz): log the star authority's error? keep
			// log of the past few for /galaxycachez?  It's
			// probably boring (normal task movement), so not
			// worth logging I imagine.
		}
		value, err = g.getLocally(ctx, key, dest)
		if err != nil {
			// TODO(@odeke-em): Remove .Stats
			g.Stats.LocalLoadErrs.Add(1)
			stats.Record(ctx, MLocalLoadErrors.M(1))
			return nil, err
		}
		// TODO(@odeke-em): Remove .Stats
		g.Stats.LocalLoads.Add(1)
		stats.Record(ctx, MLocalLoads.M(1))
		destPopulated = true // only one caller of load gets this return value
		g.populateCache(key, value, &g.mainCache)
		return value, nil
	})
	if err == nil {
		value = viewi.(ByteView)
	}
	return
}

func (g *Galaxy) getLocally(ctx context.Context, key string, dest Sink) (ByteView, error) {
	err := g.getter.Get(ctx, key, dest)
	if err != nil {
		return ByteView{}, err
	}
	return dest.view()
}

func (g *Galaxy) getFromStarAuthority(ctx context.Context, starAuth RemoteFetcher, key string) (ByteView, error) {
	req := &pb.GetRequest{
		Galaxy: g.name,
		Key:    key,
	}
	res := &pb.GetResponse{}
	err := starAuth.Fetch(ctx, req, res)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: res.Value}
	// TODO(bradfitz): use res.MinuteQps or something smart to
	// conditionally populate hotCache.  For now just do it some
	// percentage of the time.
	if rand.Intn(10) == 0 {
		g.populateCache(key, value, &g.hotCache)
	}
	return value, nil
}

func (g *Galaxy) lookupCache(key string) (value ByteView, ok bool) {
	if g.cacheBytes <= 0 {
		return
	}
	value, ok = g.mainCache.get(key)
	if ok {
		return
	}
	value, ok = g.hotCache.get(key)
	return
}

func (g *Galaxy) populateCache(key string, value ByteView, cache *cache) {
	if g.cacheBytes <= 0 {
		return
	}
	cache.add(key, value)

	// Evict items from cache(s) if necessary.
	for {
		mainBytes := g.mainCache.bytes()
		hotBytes := g.hotCache.bytes()
		if mainBytes+hotBytes <= g.cacheBytes {
			return
		}

		// TODO(bradfitz): this is good-enough-for-now logic.
		// It should be something based on measurements and/or
		// respecting the costs of different resources.
		victim := &g.mainCache
		if hotBytes > mainBytes/8 {
			victim = &g.hotCache
		}
		victim.removeOldest()
	}
}

// CacheType represents a type of cache.
type CacheType int

const (
	// MainCache is the cache for items that this star authority is the
	// owner of.
	MainCache CacheType = iota + 1

	// HotCache is the cache for items that seem popular
	// enough to replicate to this node, even though it's not the
	// owner.
	HotCache
)

// CacheStats returns stats about the provided cache within the galaxy.
func (g *Galaxy) CacheStats(which CacheType) CacheStats {
	switch which {
	case MainCache:
		return g.mainCache.stats()
	case HotCache:
		return g.hotCache.stats()
	default:
		return CacheStats{}
	}
}

// cache is a wrapper around an *lru.Cache that adds synchronization,
// makes values always be ByteView, and counts the size of all keys and
// values.
type cache struct {
	mu         sync.RWMutex
	nbytes     int64 // of all keys and values
	lru        *lru.Cache
	nhit, nget int64
	nevict     int64 // number of evictions
}

func (c *cache) stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Bytes:     c.nbytes,
		Items:     c.itemsLocked(),
		Gets:      c.nget,
		Hits:      c.nhit,
		Evictions: c.nevict,
	}
}

func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru == nil {
		c.lru = &lru.Cache{
			OnEvicted: func(key lru.Key, value interface{}) {
				val := value.(ByteView)
				c.nbytes -= int64(len(key.(string))) + int64(val.Len())
				c.nevict++
			},
		}
	}
	c.lru.Add(key, value)
	c.nbytes += int64(len(key)) + int64(value.Len())
}

func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nget++
	if c.lru == nil {
		return
	}
	vi, ok := c.lru.Get(key)
	if !ok {
		return
	}
	c.nhit++
	return vi.(ByteView), true
}

func (c *cache) removeOldest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.RemoveOldest()
	}
}

func (c *cache) bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nbytes
}

func (c *cache) items() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.itemsLocked()
}

func (c *cache) itemsLocked() int64 {
	if c.lru == nil {
		return 0
	}
	return int64(c.lru.Len())
}

// An AtomicInt is an int64 to be accessed atomically.
type AtomicInt int64

// Add atomically adds n to i.
func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

// Get atomically gets the value of i.
func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

// CacheStats are returned by stats accessors on Galaxy.
type CacheStats struct {
	Bytes     int64
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64
}