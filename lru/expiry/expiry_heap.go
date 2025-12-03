/*
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

// Package expiry provides a type-safe heap implementation that's specialized
// for handling expiring cache-entries.
package expiry

import (
	"container/heap"
	"iter"
	"math"
	"time"
	"weak"
)

type expiryHeapEnt[T any] struct {
	expiry time.Duration // time since baseTime on the heap
	handle weak.Pointer[EntryHandle[T]]
	elem   T
}

func (e *expiryHeapEnt[T]) updateLocation(i int) {
	h := e.handle.Value()
	if h == nil {
		return
	}
	h.offset = uint(i)
}

type expiryHeap[T any] struct {
	ents []expiryHeapEnt[T]
}

// Len is the number of elements in the collection.
func (e *expiryHeap[T]) Len() int {
	return len(e.ents)
}

// Less reports whether the element with index i
// must sort before the element with index j.
//
// If both Less(i, j) and Less(j, i) are false,
// then the elements at index i and j are considered equal.
// Sort may place equal elements in any order in the final result,
// while Stable preserves the original input order of equal elements.
//
// Less must describe a transitive ordering:
//   - if both Less(i, j) and Less(j, k) are true, then Less(i, k) must be true as well.
//   - if both Less(i, j) and Less(j, k) are false, then Less(i, k) must be false as well.
//
// Note that floating-point comparison (the < operator on float32 or float64 values)
// is not a transitive ordering when not-a-number (NaN) values are involved.
// See Float64Slice.Less for a correct implementation for floating-point values.
func (e *expiryHeap[T]) Less(i int, j int) bool {
	return e.ents[i].expiry < e.ents[j].expiry
}

// Swap swaps the elements with indexes i and j.
func (e *expiryHeap[T]) Swap(i, j int) {
	e.ents[j].updateLocation(i)
	e.ents[i].updateLocation(j)
	e.ents[i], e.ents[j] = e.ents[j], e.ents[i]
}

// Push implements [heap.Interface], adding x to the end of the heap.
// It is plumbing for [container/heap].
//
// Use [heap.Push] instead.
func (e *expiryHeap[T]) Push(x any) {
	xt := x.(expiryHeapEnt[T])
	e.ents = append(e.ents, xt)
	xt.updateLocation(len(e.ents) - 1)
}

// sentinelOffset is used to indicate that an entry is no longer present in the map.
// It will be converted to an int, so it needs to be within the signed range to
// have a directly comparable value.
const sentinelOffset = math.MaxInt

// Push implements [heap.Interface], removing and returning the entry at the end of the heap.
// It is plumbing for [container/heap].
//
// Use [heap.Pop] instead.
func (e *expiryHeap[T]) Pop() any {
	v := e.ents[len(e.ents)-1]
	e.ents = e.ents[:len(e.ents)-1]
	v.updateLocation(sentinelOffset)
	return v
}

// EntryHandle provides a handle for the resource in the heap to allow one to
// remove entries later.
//
// Note: although a pointer to this struct is returned by [ExpiryTracker.Push],
// it's expected that synchronization is handled by the caller.
type EntryHandle[T any] struct {
	// note: not atomic because the entire ExpiryTracker requires external
	// synchronization anyway.
	offset uint
}

// ExpiryTracker maintains a priority queue of payloads with their expiration times.
type ExpiryTracker[T any] struct {
	baseTime time.Time
	heap     expiryHeap[T]
}

// Push inserts a new entry
func (e *ExpiryTracker[T]) Push(expiry time.Time, ent T) *EntryHandle[T] {
	if e.baseTime.IsZero() {
		e.baseTime = expiry
	}
	handle := EntryHandle[T]{}
	heap.Push(&e.heap, expiryHeapEnt[T]{expiry: expiry.Sub(e.baseTime), elem: ent, handle: weak.Make(&handle)})
	return &handle
}

// Remove deletes the specified entry from the expiry heap.
func (e *ExpiryTracker[T]) Remove(h *EntryHandle[T]) {
	if h == nil {
		// don't try to be too clever with nil pointers
		return
	}
	offset := int(h.offset)
	if offset == sentinelOffset || offset > len(e.heap.ents)-1 {
		// If the offset now points past the end of the slice then we're done.
		return
	}
	v := e.heap.ents[offset]
	if v.handle.Value() != h {
		// This is a stale handle, don't do anything more
		return
	}
	heap.Remove(&e.heap, offset)
	// Make sure a double-Remove of this value exits early
	h.offset = sentinelOffset
}

// PopAllExpired provides an iterator that yields all entries that have already expired
func (e *ExpiryTracker[T]) PopAllExpired(now time.Time) iter.Seq[T] {
	nowBase := now.Sub(e.baseTime)
	return func(yield func(T) bool) {
		for len(e.heap.ents) > 0 && e.heap.ents[0].expiry <= nowBase {
			ne := heap.Pop(&e.heap)
			if !yield(ne.(expiryHeapEnt[T]).elem) {
				return
			}
		}
	}
}
