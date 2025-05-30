//go:build go1.24

/*
Copyright 2013 Google Inc.
Copyright 2025 Vimeo Inc.

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

package lru

import (
	"time"
	"weak"

	"github.com/vimeo/go-clocks"

	"github.com/vimeo/galaxycache/lru/expiry"
)

type typedEntry[K comparable, V any] struct {
	key       K
	value     V
	expiry    time.Duration // relative to expiryBase on TypedCache
	hasExpiry bool
}

// TypedCache is an LRU cache. It is not safe for concurrent access.
type TypedCache[K comparable, V any] struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specificies a callback function to be
	// executed when an typedEntry is purged from the cache.
	OnEvicted func(key K, value V)

	// clock against which to check for item expirty
	Clock clocks.Clock

	// cache comes first so the GC enqueues marking the map-contents first
	// (which will mark the contents of the linked-list much more
	// efficiently than traversing the linked-list directly)
	cache map[K]*llElem[typedEntry[K, V]]
	ll    linkedList[typedEntry[K, V]]

	expirations expiry.ExpiryTracker[weak.Pointer[llElem[typedEntry[K, V]]]]
	expiryBase  time.Time
}

func (c *TypedCache[K, V]) now() time.Time {
	if c.Clock == nil {
		return time.Now()
	}
	return c.Clock.Now()
}

func (c *TypedCache[K, V]) nowBase() time.Duration {
	return c.now().Sub(c.expiryBase)
}

// Remove removes the provided key from the cache.
func (c *TypedCache[K, V]) Remove(key K) {
	if c.cache == nil {
		return
	}
	c.removeExpired()
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *TypedCache[K, V]) RemoveOldest() {
	if c.cache == nil {
		return
	}
	c.removeExpired()
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *TypedCache[K, V]) removeExpired() {
	if c.cache == nil {
		return
	}
	for candidate := range c.expirations.PopAllExpired(c.now()) {
		ele := candidate.Value()
		if ele == nil {
			continue
		}
		c.removeElement(ele)
	}
}

// Add adds a value to the cache.
func (c *TypedCache[K, V]) Add(key K, value V) {
	if c.cache == nil {
		c.cache = make(map[K]*llElem[typedEntry[K, V]])
	} else {
		c.removeExpired()
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		ele.value.value = value
		return
	}
	ele := c.ll.PushFront(typedEntry[K, V]{key, value, 0, false})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

// AddExpiring provides the ability to insert an entry that expires at the timestamp [expiration]
func (c *TypedCache[K, V]) AddExpiring(key K, value V, expiration time.Time) {
	if c.cache == nil {
		c.cache = make(map[K]*llElem[typedEntry[K, V]])
	} else {
		c.removeExpired()
	}
	if c.expiryBase.IsZero() {
		c.expiryBase = expiration
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		ele.value.value = value
		if !expiration.IsZero() {
			c.expirations.Push(expiration, weak.Make(ele))
		}
		return
	}
	ele := c.ll.PushFront(typedEntry[K, V]{key, value, expiration.Sub(c.expiryBase), !expiration.IsZero()})
	c.cache[key] = ele
	if !expiration.IsZero() {
		c.expirations.Push(expiration, weak.Make(ele))
	}
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

// Get looks up a key's value from the cache.
func (c *TypedCache[K, V]) Get(key K) (value V, ok bool) {
	value, _, ok = c.GetWithExpiry(key)
	return
}

// GetWithExpiry looks up a key's value from the cache.
func (c *TypedCache[K, V]) GetWithExpiry(key K) (value V, exp time.Time, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		if ele.value.hasExpiry {
			if ele.value.expiry <= c.nowBase() {
				// It's here, but it expired. Remove it and reap the
				// other expired entries while we're at it
				c.removeElement(ele)
				c.removeExpired()
				return
			}
			exp = c.expiryBase.Add(ele.value.expiry)
		}
		c.ll.MoveToFront(ele)
		return ele.value.value, exp, true
	}
	return
}

// MostRecent returns the most recently used element
func (c *TypedCache[K, V]) MostRecent() *V {
	for c.Len() > 0 {
		ele := c.ll.Front()
		if ele.value.hasExpiry && ele.value.expiry <= c.nowBase() {
			c.removeElement(ele)
			c.removeExpired()
			continue
		}
		return &ele.value.value
	}
	return nil
}

// LeastRecent returns the least recently used element
func (c *TypedCache[K, V]) LeastRecent() *V {
	for c.Len() > 0 {
		ele := c.ll.Back()
		if ele.value.hasExpiry && ele.value.expiry <= c.nowBase() {
			c.removeElement(ele)
			c.removeExpired()
			continue
		}
		return &ele.value.value
	}
	return nil
}
