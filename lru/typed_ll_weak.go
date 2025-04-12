//go:build go1.24

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

package lru

import (
	"fmt"
	"weak"
)

// LinkedList using generics to reduce the number of heap objects
// Used for the LRU stack in TypedCache
// This implementation switches to using weak pointers when using go 1.24+ so
// the GC can skip scanning the linked list elements themselves.
type linkedList[T any] struct {
	head weak.Pointer[llElem[T]]
	tail weak.Pointer[llElem[T]]
	size int
}

type llElem[T any] struct {
	value      T
	next, prev weak.Pointer[llElem[T]]
}

func (l *llElem[T]) Next() *llElem[T] {
	return l.next.Value()
}

func (l *llElem[T]) Prev() *llElem[T] {
	return l.prev.Value()
}

func (l *linkedList[T]) PushFront(val T) *llElem[T] {
	if paranoidLL {
		l.checkHeadTail()
		defer l.checkHeadTail()
	}
	elem := llElem[T]{
		next:  l.head,
		prev:  weak.Pointer[llElem[T]]{}, // first element
		value: val,
	}
	weakElem := weak.Make(&elem)
	if lHead := l.head.Value(); lHead != nil {
		lHead.prev = weakElem
	}
	if lTail := l.tail.Value(); lTail == nil {
		l.tail = weakElem
	}
	l.head = weakElem
	l.size++

	return &elem
}

func (l *linkedList[T]) MoveToFront(e *llElem[T]) {
	if paranoidLL {
		if e == nil {
			panic("nil element")
		}
		l.checkHeadTail()
		defer l.checkHeadTail()
	}

	extHead := l.head.Value()

	if extHead == e {
		// nothing to do
		return
	}
	eWeak := weak.Make(e)

	if eNext := e.next.Value(); eNext != nil {
		// update the previous pointer on the next element
		eNext.prev = e.prev
	}
	if ePrev := e.prev.Value(); ePrev != nil {
		ePrev.next = e.next
	}
	if lHead := l.head.Value(); lHead != nil {
		lHead.prev = eWeak
	}

	if lTail := l.tail.Value(); lTail == e {
		l.tail = e.prev
	}
	e.next = l.head
	l.head = eWeak
	e.prev = weak.Pointer[llElem[T]]{}
}

func (l *linkedList[T]) checkHeadTail() {
	if !paranoidLL {
		return
	}
	if (l.head.Value() != nil) != (l.tail.Value() != nil) {
		panic(fmt.Sprintf("invariant failure; nilness mismatch: head: %+v; tail: %+v (size %d)",
			l.head, l.tail, l.size))
	}

	if l.size > 0 && (l.head.Value() == nil || l.tail.Value() == nil) {
		panic(fmt.Sprintf("invariant failure; head and/or tail nil with %d size: head: %+v; tail: %+v",
			l.size, l.head, l.tail))
	}

	if lHead := l.head.Value(); lHead != nil && (lHead.prev.Value() != nil || (lHead.next.Value() == nil && l.size != 1)) {
		panic(fmt.Sprintf("invariant failure; head next/prev invalid with %d size: head: %+v; tail: %+v",
			l.size, l.head, l.tail))
	}
	if lTail := l.tail.Value(); lTail != nil && ((lTail.prev.Value() == nil && l.size != 1) || lTail.next.Value() != nil) {
		panic(fmt.Sprintf("invariant failure; tail next/prev invalid with %d size: head: %+v; tail: %+v",
			l.size, l.head, l.tail))
	}
}

func (l *linkedList[T]) Remove(e *llElem[T]) {
	if paranoidLL {
		if e == nil {
			panic("nil element")
		}
		l.checkHeadTail()
		defer l.checkHeadTail()
	}
	if l.tail.Value() == e {
		l.tail = e.prev
	}
	if l.head.Value() == e {
		l.head = e.next
	}

	if eNext := e.next.Value(); eNext != nil {
		// update the previous pointer on the next element
		eNext.prev = e.prev
	}
	if ePrev := e.prev.Value(); ePrev != nil {
		ePrev.next = e.next
	}
	l.size--
}

func (l *linkedList[T]) Len() int {
	return l.size
}

func (l *linkedList[T]) Front() *llElem[T] {
	return l.head.Value()
}

func (l *linkedList[T]) Back() *llElem[T] {
	return l.tail.Value()
}
