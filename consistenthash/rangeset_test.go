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
	"bytes"
	"testing"
)

func TestGetOwnwerRangeSummary(t *testing.T) {
	type keyReplID struct {
		k string
		r int
	}
	for _, tbl := range []struct {
		name             string
		baseHashes       map[string]uint32
		replHashes       map[keyReplID]uint32
		segsPerKey       int // most cases will set this to 1 (must be < 128 for test-purposes)
		ownerKeys        []string
		qReplicas        int // must be >1 to do anything interesting
		skipOverrideKeys int
		checkOwner       string
		// map from key to whether the owner in checkOwner should return true for the vaue of skipOverrideKeys
		checkKeys       map[string]bool
		expectSummary   bool
		expectFullRange bool
	}{
		{
			name:       "simple_1_owner",
			ownerKeys:  []string{"apple"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10,
				"fizzle": 5, // put fizzle in apple's range
			},
			qReplicas: 2,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 35,
			},
			checkOwner:       "apple",
			skipOverrideKeys: 0,
			checkKeys:        map[string]bool{"fizzle": true},
			expectSummary:    true,
			expectFullRange:  true,
		},
		{
			name:       "simple_1_owner_3_segs",
			ownerKeys:  []string{"apple"},
			segsPerKey: 3,
			baseHashes: map[string]uint32{
				"0apple": 10,
				"1apple": 15, // make sure the ranges get merged
				"2apple": 0xff,
				"fizzle": 5, // put fizzle in apple's range
			},
			qReplicas: 2,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 35,
			},
			checkOwner:       "apple",
			skipOverrideKeys: 0,
			checkKeys:        map[string]bool{"fizzle": true},
			expectSummary:    true,
			expectFullRange:  true,
		},
		{
			name:       "3_owners_3_segs_merge_ranges_skip_override_keys_1",
			ownerKeys:  []string{"apple", "orange", "pear"},
			segsPerKey: 3,
			baseHashes: map[string]uint32{
				"0apple":  10,
				"0orange": 12,
				"1apple":  15, // make sure the ranges get merged
				"1orange": 32,
				"0pear":   50,
				"1pear":   55,
				"2apple":  0xff,
				"2pear":   800,
				"fizzle":  12, // put fizzle in orange's range
			},
			qReplicas:        1,
			replHashes:       map[keyReplID]uint32{},
			checkOwner:       "apple",
			skipOverrideKeys: 1,
			checkKeys:        map[string]bool{"fizzle": true},
			expectSummary:    true,
			expectFullRange:  false,
		},
		{
			name:       "3_owners_3_segs_merge_ranges_skip_override_keys_1_4_replicas_trivially_all_covered",
			ownerKeys:  []string{"apple", "orange", "pear"},
			segsPerKey: 3,
			baseHashes: map[string]uint32{
				"0apple":  10,
				"0orange": 12,
				"1apple":  15,
				"1orange": 32,
				"0pear":   50,
				"1pear":   55,
				"2apple":  0xffff,
				"2pear":   800,
				"fizzle":  32, // put fizzle in orange's range
			},
			qReplicas: 4,
			replHashes: map[keyReplID]uint32{
				{"fizzle", 1}: 32,
				{"fizzle", 2}: 32,
				{"fizzle", 3}: 32,
			},
			checkOwner:       "apple",
			skipOverrideKeys: 1,
			checkKeys:        map[string]bool{"fizzle": true},
			expectSummary:    true,
			expectFullRange:  false,
		},
		{
			name:       "3_owners_3_segs_merge_ranges_skip_override_keys_3",
			ownerKeys:  []string{"apple", "orange", "pear"},
			segsPerKey: 3,
			baseHashes: map[string]uint32{
				"0apple":  10,
				"0orange": 12,
				"1apple":  15, // make sure the ranges get merged
				"1orange": 32,
				"0pear":   50,
				"1pear":   55,
				"2apple":  0xff,
				"2pear":   800,
				"fizzle":  12, // put fizzle in orange's range
			},
			qReplicas:        1,
			replHashes:       map[keyReplID]uint32{},
			checkOwner:       "apple",
			skipOverrideKeys: 3,
			checkKeys:        map[string]bool{"fizzle": true},
			expectSummary:    true,
			expectFullRange:  true,
		},
		{
			name:       "unknown_key",
			ownerKeys:  []string{"apple"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10,
				"fizzle": 5, // put fizzle in apple's range
			},
			qReplicas: 2,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 35,
			},
			checkOwner:       "fruit",
			skipOverrideKeys: 0,
			expectSummary:    false,
		},
		{
			name:       "simple_2_replicas_no_skip_override",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"fizzle":  5,  // put fizzle in apple's range
				"fooble":  29, // put fooble in pear's range
				"bippity": 50, // put bippity in pear's range
				"boop":    55, // put boop in plum's range
			},
			qReplicas: 2,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}:  35, // strawberry
				{k: "fooble", r: 1}:  36, // strawberry
				{k: "bippity", r: 1}: 49, // peach->plum
				{k: "boop", r: 1}:    51, // plum->apple
			},
			checkOwner:       "apple",
			skipOverrideKeys: 0,
			checkKeys: map[string]bool{
				"fizzle":  true,
				"fooble":  false,
				"bippity": false,
				"boop":    true,
			},
			expectSummary:   true,
			expectFullRange: false,
		},
		{
			name:       "collision_2_replicas_no_skip_override",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 20, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"fizzle": 5,  // put fizzle in apple's range
				"fooble": 19, // put fooble in pear's range
			},
			qReplicas: 2,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 35, // strawberry
				{k: "fooble", r: 1}: 36, // strawberry
			},
			checkOwner:       "orange",
			skipOverrideKeys: 0,
			checkKeys: map[string]bool{
				"fizzle": false,
				"fooble": false,
			},
			expectSummary:   true,
			expectFullRange: false,
		},
		{
			name:       "3_replicas_skip_override_1",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 2,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"1apple": 110, "1orange": 120,
				"1pear": 130, "1strawberry": 140,
				"1peach": 150, "1plum": 160,
				"fizzle":  5,  // put fizzle in apple's range
				"fooble":  29, // put fooble in pear's range
				"bippity": 50, // put bippity in pear's range
				"boop":    55, // put boop in plum's range
			},
			qReplicas: 3,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}:  35,  // strawberry
				{k: "fooble", r: 1}:  36,  // strawberry
				{k: "bippity", r: 1}: 49,  // peach->plum
				{k: "boop", r: 1}:    51,  // plum->apple
				{k: "fizzle", r: 2}:  135, // strawberry->peach
				{k: "fooble", r: 2}:  136, // strawberry->peach
				{k: "bippity", r: 2}: 149, // peach->plum->apple
				{k: "boop", r: 2}:    151, // plum->apple->orange
			},
			checkOwner:       "apple",
			skipOverrideKeys: 1,
			checkKeys: map[string]bool{
				"fizzle":  true,
				"fooble":  false,
				"bippity": true,
				"boop":    true,
			},
			expectSummary:   true,
			expectFullRange: false,
		},
		{
			name:       "3_replicas_skip_override_1_wrap_around",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 2,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"1apple": 110, "1orange": 120,
				"1pear": 130, "1strawberry": 140,
				"1peach": 150, "1plum": 160,
				"fizzle":  5,  // put fizzle in apple's range
				"fooble":  29, // put fooble in pear's range
				"bippity": 50, // put bippity in peach's range
				"boop":    55, // put boop in plum's range
			},
			qReplicas: 3,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}:  29,  // pear
				{k: "fooble", r: 1}:  36,  // strawberry
				{k: "bippity", r: 1}: 49,  // peach->plum
				{k: "boop", r: 1}:    51,  // plum->apple
				{k: "fizzle", r: 2}:  135, // strawberry
				{k: "fooble", r: 2}:  136, // strawberry->peach
				{k: "bippity", r: 2}: 149, // peach->plum->apple
				{k: "boop", r: 2}:    151, // plum->apple->orange
			},
			checkOwner:       "plum",
			skipOverrideKeys: 1,
			checkKeys: map[string]bool{
				"fizzle":  false,
				"fooble":  true,
				"bippity": true,
				"boop":    true,
			},
			expectSummary:   true,
			expectFullRange: false,
		},
		{
			name:       "2_replicas_skip_override_3_wrap_around",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"fizzle":  5,  // put fizzle in apple's range
				"fooble":  20, // put fooble in orange's range
				"bippity": 50, // put bippity in peach's range
				"boop":    55, // put boop in plum's range
			},
			qReplicas: 2,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}:  19, // orange
				{k: "fooble", r: 1}:  8,  // apple
				{k: "bippity", r: 1}: 49, // peach->plum
				{k: "boop", r: 1}:    51, // plum->apple
			},
			checkOwner:       "plum",
			skipOverrideKeys: 3,
			checkKeys: map[string]bool{
				"fizzle":  false,
				"fooble":  false,
				"bippity": true,
				"boop":    true,
			},
			expectSummary:   true,
			expectFullRange: false,
		},
	} {
		t.Run(tbl.name, func(t *testing.T) {
			hashFn := func(b []byte) uint32 {
				bk, suf, ok := bytes.Cut(b, []byte{0xaa, 0xaa})
				if !ok {
					return tbl.baseHashes[string(b)]
				}
				if len(suf) != 1 || suf[0] > 127 {
					panic("varint suffix too long (replicas > 127?)")
				}
				return tbl.replHashes[keyReplID{k: string(bk), r: int(suf[0])}]
			}
			hr := New(tbl.segsPerKey, hashFn)
			hr.Add(tbl.ownerKeys...)
			summary, ok := hr.OwnerRangeSummary(tbl.checkOwner, tbl.skipOverrideKeys)
			if !ok {
				if tbl.expectSummary {
					t.Error("summary failed")
				}
				return
			}
			if summary.wholeKeyspace != tbl.expectFullRange {
				t.Errorf("whole keyspace mismatch: got %t; want %t", summary.wholeKeyspace, tbl.expectFullRange)
			}
			for k, expOwned := range tbl.checkKeys {
				if posOwned := summary.OwnerPossiblyOwns(k, tbl.qReplicas); posOwned != expOwned {
					t.Errorf("key %q has unexpected possible ownership; got %t; want %t", k, posOwned, expOwned)
					t.Logf("owned ranges for %q: %v", tbl.checkOwner, summary.rs.ranges)
					t.Log("hashes:")
					for own, h := range summary.m.getReplicated(k, tbl.qReplicas) {
						t.Log(own, h)
					}
				}
			}
		})
	}
}
