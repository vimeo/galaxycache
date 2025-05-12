//go:build go1.18

/*
Copyright 2013 Google Inc.
Copyright 2022-2025 Vimeo Inc.

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

// Package lru implements an LRU cache.
package lru // import "github.com/vimeo/galaxycache/lru"

// TypedNew creates a new Cache (with types).
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func TypedNew[K comparable, V any](maxEntries int) *TypedCache[K, V] {
	return &TypedCache[K, V]{
		MaxEntries: maxEntries,
		cache:      make(map[K]*llElem[typedEntry[K, V]]),
	}
}

func (c *TypedCache[K, V]) removeElement(e *llElem[typedEntry[K, V]]) {
	kv := e.value
	if _, ok := c.cache[kv.key]; !ok {
		// This entry's been removed already (it might have expired
		// _much_ later than it was removed for another reason).
		return
	}
	c.ll.Remove(e)
	// Wait until after we've removed the element from the linked list
	// before removing from the map so we can leverage weak pointers in
	// the linked list/LRU stack.
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

// Len returns the number of items in the cache.
func (c *TypedCache[K, V]) Len() int {
	return c.ll.Len()
}

// Clear purges all stored items from the cache.
func (c *TypedCache[K, V]) Clear() {
	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.value
			c.OnEvicted(kv.key, kv.value)
		}
	}
	c.ll = linkedList[typedEntry[K, V]]{}
	c.cache = nil
}
