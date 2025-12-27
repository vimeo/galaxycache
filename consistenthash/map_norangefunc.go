//go:build !go1.23

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

package consistenthash

import (
	"sort"
	"strconv"
)

// Add adds some keys to the hashring, establishing ownership of segsPerKey
// segments.
func (m *Map) Add(keys ...string) {
	hashToOwner := make(map[uint32]string, len(m.segments)+len(keys)*m.segsPerKey)
	for _, seg := range m.segments {
		hashToOwner[seg.hash] = seg.owner
	}

	for _, key := range keys {
		m.keys[key] = struct{}{}
		for i := 0; i < m.segsPerKey; i++ {
			hash := m.hash([]byte(strconv.Itoa(i) + key))
			// If there's a collision on a "replica" (segment-boundary), we only want
			// the entry that sorts latest to get inserted (not the last one we saw).
			//
			// It doesn't matter how we reconcile collisions (the smallest would work
			// just as well), we just need it to be insertion-order independent so all
			// instances converge on the same hashmap.
			if extKey, ok := hashToOwner[hash]; ok && extKey >= key {
				continue
			}
			hashToOwner[hash] = key
		}
	}

	m.segments = make([]segment, 0, len(hashToOwner))
	for hash, owner := range hashToOwner {
		m.segments = append(m.segments, segment{hash: hash, owner: owner})
	}
	sort.Slice(m.segments, func(i, j int) bool { return m.segments[i].hash < m.segments[j].hash })
}

// GetReplicated gets the closest item in the hash to a deterministic set of
// keyReplicas variations of the provided key.
// The returned set of segment-owning keys is dedup'd, and collisions are
// resolved by traversing backwards in the hash-ring to find an unused
// owning-key.
func (m *Map) GetReplicated(key string, keyReplicas int) []string {
	if m.IsEmpty() {
		return []string{}
	}
	out := make([]string, 0, keyReplicas)
	segOwners := make(map[string]struct{}, keyReplicas)

	for i := 0; i < keyReplicas && len(out) < len(m.keys); i++ {
		h := m.idxedKeyReplica(key, i)
		segIdx, _, owner := m.findSegmentOwner(h)
		for _, present := segOwners[owner]; present; _, present = segOwners[owner] {
			// this may overflow, which is fine.
			segIdx, _, owner = m.nextSegmentOwner(segIdx)
		}
		segOwners[owner] = struct{}{}
		out = append(out, owner)
	}

	return out
}
