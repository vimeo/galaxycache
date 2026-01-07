/*
Copyright 2013 Google Inc.
Copyright 2020-2025 Vimeo Inc.

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

// Package consistenthash provides an implementation of a ring hash.
package consistenthash // import "github.com/vimeo/galaxycache/consistenthash"

import (
	"encoding/binary"
	"hash/crc32"
	"hash/fnv"
	"sort"
)

// FNVHash is an alternate hash function for [Map], and wraps/adapts [fnv.New32a].
// It is generally a better choice of hash function than the default crc32 with
// the IEEE polynomial ([crc32.ChecksumIEEE]). (the default cannot be updated
// without breaking existing systems)
func FNVHash(data []byte) uint32 {
	f := fnv.New32a()
	f.Write(data)
	return f.Sum32()
}

// Hash maps the data to a uint32 hash-ring
type Hash func(data []byte) uint32

// segment represents a hash ring entry containing both the hash value and the owner.
type segment struct {
	hash  uint32
	owner string
}

// Map tracks segments in a hash-ring, mapped to specific keys.
type Map struct {
	hash       Hash
	segsPerKey int
	// segments stores the sorted entries in the hash-ring, each containing
	// the hash upper-bound and the owner-key-name.
	segments []segment
	// keys tracks which owner-keys are currently present in the hash-ring
	keys map[string]struct{}
}

// New constructs a new consistenthash hashring, with segsPerKey segments per added key.
// It is recommended to use [FNVHash] as the second (fn) argument for new applications/systems.
func New(segsPerKey int, fn Hash) *Map {
	m := &Map{
		segsPerKey: segsPerKey,
		hash:       fn,
		keys:       make(map[string]struct{}),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.segments) == 0
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := m.hash([]byte(key))

	_, _, owner := m.findSegmentOwner(hash)
	return owner
}

func (m *Map) findSegmentOwner(hash uint32) (int, uint32, string) {
	// Binary search for appropriate replica.
	idx := sort.Search(len(m.segments), func(i int) bool { return m.segments[i].hash >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(m.segments) {
		idx = 0
	}

	return idx, m.segments[idx].hash, m.segments[idx].owner
}

func (m *Map) nextSegmentOwner(idx int) (int, uint32, string) {
	if len(m.keys) == 1 {
		panic("attempt to find alternate owner for single-key map")
	}
	if idx == len(m.segments)-1 {
		// if idx is len(m.keys)-1, then wrap around
		return 0, m.segments[0].hash, m.segments[0].owner
	}

	// we're moving forward within a ring; increment the index
	idx++

	return idx, m.segments[idx].hash, m.segments[idx].owner
}

func (m *Map) idxedKeyReplica(key string, replica int) uint32 {
	// For replica zero, do not append a suffix so Get() and GetReplicated are compatible
	if replica == 0 {
		return m.hash([]byte(key))
	}
	// Allocate an extra 2 bytes so we have 2 bytes of padding to function
	// as a separator between the main key and the suffix
	idxSuffixBuf := [binary.MaxVarintLen64 + 2]byte{}
	// Set those 2 bytes of padding to a nice non-zero value with
	// alternating zeros and ones.
	idxSuffixBuf[0] = 0xaa
	idxSuffixBuf[1] = 0xaa

	// Encode the replica using unsigned varints which are more compact and cheaper to encode.
	// definition: https://developers.google.com/protocol-buffers/docs/encoding#varints
	vIntLen := binary.PutUvarint(idxSuffixBuf[2:], uint64(replica))

	idxHashKey := append([]byte(key), idxSuffixBuf[:vIntLen+2]...)
	return m.hash(idxHashKey)
}
