//go:build go1.23

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
	"cmp"
	"maps"
	"math"
	"slices"
)

// ownerRange is a closed range [low, high]
type ownerRange struct {
	low  uint32
	high uint32
}

type rangeSet struct {
	ranges []ownerRange
}

func newRangeSet(ranges []ownerRange) rangeSet {
	if len(ranges) <= 1 {
		return rangeSet{ranges: ranges}
	}
	slices.SortFunc(ranges, func(a, b ownerRange) int {
		return cmp.Compare(a.low, b.low)
	})
	outRanges := make([]ownerRange, 1, len(ranges))
	outRanges[0] = ranges[0]

	for _, elt := range ranges[1:] {
		tailRange := outRanges[len(outRanges)-1]
		// Check whether there's even a chance that these overlap.
		// The elements are sorted by the "low" value, so we don't need to compare the lows here.
		// If high of the left-hand (lower) range is greater than (or 1-less-than) the low of the right-hand range, then they overlap.
		if tailRange.high < elt.low-1 {
			// they don't overlap, so just flush this range into outRanges
			outRanges = append(outRanges, elt)
			continue
		}
		// It overlaps, so we now need to merge the ranges (set the high of the tail in outRanges to the max of the two highs)
		outRanges[len(outRanges)-1].high = max(tailRange.high, elt.high)
	}
	return rangeSet{ranges: outRanges}
}

func (r *rangeSet) ownedhash(h uint32) bool {
	_, ok := slices.BinarySearchFunc(r.ranges, h, func(or ownerRange, v uint32) int {
		if or.low <= v && or.high >= v {
			return 0
		}
		return cmp.Compare(or.low, v)
	})
	return ok
}

// OwnerRangeSummary provides an object that provides an efficient check as to
// whether a key is owned by a specific owner-key. (where owner-keys would be
// provided to [(*Map).Add] and a `key` would be passed to [(*Map).Get]).
//
// skipOverrideKeys allows one to provide a broader bound on ownership that
// includes the removal of `skipOverrideKeys` owners.
//
// Returns nil if the key is not present in this map.
func (m *Map) OwnerRangeSummary(ownerKey string, skipOverrideKeys int) (RangeSummary, bool) {
	if _, ok := m.keys[ownerKey]; !ok {
		// unknown owner-key
		return RangeSummary{}, false
	}
	if skipOverrideKeys >= len(m.keys)-1 {
		return RangeSummary{wholeKeyspace: true}, true
	}
	// Make sure the value makes sense (although, if it's too large we'll just return one big range after doing a lot of work)
	skipOverrideKeys = min(len(m.keys)-1, skipOverrideKeys)
	ranges := make([]ownerRange, 0, m.segsPerKey)
	for hash := range m.ownerKeyHashes(ownerKey) {
		if skipOverrideKeys == 0 && m.hashMap[hash] != ownerKey {
			// this ownerKey doesn't own this hash, and we're assuming that no one else is leaving the ring
			continue
		}
		idx, _, _ := m.findSegmentOwner(hash)
		lowerBound := uint32(0)
		if idx <= skipOverrideKeys {
			// too close to the beginning of the ring to simply index
			// we have to wrap around.
			wrappedLowerIdx := len(m.keyHashes) - (skipOverrideKeys - idx) - 1
			// first hash, so we'll wrap around for the lower-bound.
			// lowerBound is already set to 0, so we don't need to touch that, but we do need to push a
			// range for the tail of the last range in keyHashes.
			// ... but, only if the last key isn't MaxUint32.
			if m.keyHashes[wrappedLowerIdx] != math.MaxUint32 {
				ranges = append(ranges, ownerRange{low: m.keyHashes[wrappedLowerIdx] + 1, high: math.MaxUint32})
			}
		} else {
			// we're guaranteed to be able to index into the previous entry (and add 1 to it)
			lowerBound = m.keyHashes[idx-skipOverrideKeys-1] + 1
		}
		// The upper-bound is fixed at this key's value. We're only traversing to lower values
		// now, take care of filling in the upper-bound
		ranges = append(ranges, ownerRange{low: lowerBound, high: hash})
	}

	rs := newRangeSet(ranges)

	return RangeSummary{
		rs:               rs,
		skipOverrideKeys: skipOverrideKeys,
		m: Map{
			hash:       m.hash,
			segsPerKey: m.segsPerKey,
			keyHashes:  slices.Clone(m.keyHashes),
			hashMap:    maps.Clone(m.hashMap),
			keys:       maps.Clone(m.keys),
		},
	}, true
}

type RangeSummary struct {
	rs               rangeSet
	skipOverrideKeys int
	m                Map // copy of the original map (readonly to allow for concurrent use)
	wholeKeyspace    bool
}

func (r *RangeSummary) OwnerPossiblyOwns(key string, replicas int) bool {
	if r.wholeKeyspace {
		return true
	}
	if replicas >= len(r.m.keys) {
		// There are more replicas than key-owners, so this key
		// trivially owns the whole keyspace (just like everyone else)
		return true
	}
	// make a fast-pass to see if we are a trivial owner of this range (this also takes care of replicas = 1
	// cheaply)
	for i := range replicas {
		h := r.m.idxedKeyReplica(key, i)
		if r.rs.ownedhash(h) {
			// unambiguously owned (for the value of of skipOverrideKeys)
			return true
		}
	}
	if replicas < r.skipOverrideKeys {
		// skipOverrideKeys already covers any "collisions"
		return false
	}
	// skipOverrideKeys isn't large enough to guarantee that we haven't gone through a number of other
	// replicas and landed in one of our ranges. Iterate over getReplicated's values and check whether any of the
	// hashes of those segment-bounds lands in this owner's ranges. (once you account for skipOverrideKeys)
	for _, h := range r.m.getReplicated(key, replicas) {
		if r.rs.ownedhash(h) {
			return true
		}
	}
	return false
}
