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
	"time"
)

type expiryHeapEnt[T any] struct {
	expiry time.Duration // time since baseTime on the heap
	elem   T
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
	e.ents[i], e.ents[j] = e.ents[j], e.ents[i]
}

func (e *expiryHeap[T]) Push(x any) {
	xt := x.(expiryHeapEnt[T])
	e.ents = append(e.ents, xt)
}

func (e *expiryHeap[T]) Pop() any {
	v := e.ents[len(e.ents)-1]
	e.ents = e.ents[:len(e.ents)-1]
	return v
}

// ExpiryTracker maintains a priority queue of payloads with their expiration times.
type ExpiryTracker[T any] struct {
	baseTime time.Time
	heap     expiryHeap[T]
}

// Push inserts a new entry
func (e *ExpiryTracker[T]) Push(expiry time.Time, ent T) {
	if e.baseTime.IsZero() {
		e.baseTime = expiry
	}
	heap.Push(&e.heap, expiryHeapEnt[T]{expiry: expiry.Sub(e.baseTime), elem: ent})
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
