/*
Copyright 2013 Google Inc.

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
	"fmt"
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {

	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := New(3, func(key []byte) uint32 {
		i, err := strconv.Atoi(string(key))
		if err != nil {
			panic(err)
		}
		return uint32(i)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.Add("6", "4", "2")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Adds 8, 18, 28
	hash.Add("8")

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

}

func TestHashCollision(t *testing.T) {
	hashFunc := func(d []byte) uint32 {
		return uint32(d[0])
	}
	hash1 := New(1, hashFunc)
	hash1.Add("Bill", "Bob", "Bonny")
	hash2 := New(1, hashFunc)
	hash2.Add("Bill", "Bonny", "Bob")

	// Helper to find owner by hash value in segments
	findOwner := func(m *Map, h uint32) string {
		for _, seg := range m.segments {
			if seg.hash == h {
				return seg.owner
			}
		}
		return ""
	}

	t.Log(findOwner(hash1, uint32('0')), findOwner(hash2, uint32('0')))
	t.Logf("%+v", hash1.segments)
	t.Logf("%+v", hash2.segments)
	if findOwner(hash1, uint32('0')) != findOwner(hash2, uint32('0')) {
		t.Errorf("inconsistent owner for hash %d: %s vs %s", 'B',
			findOwner(hash1, uint32('B')), findOwner(hash2, uint32('B')))
	}
}

func TestConsistency(t *testing.T) {
	hash1 := New(1, nil)
	hash2 := New(1, nil)

	hash1.Add("Bill", "Bob", "Bonny")
	hash2.Add("Bob", "Bonny", "Bill")

	if hash1.Get("Ben") != hash2.Get("Ben") {
		t.Errorf("Fetching 'Ben' from both hashes should be the same")
	}

	hash2.Add("Becky", "Ben", "Bobby")

	if hash1.Get("Ben") != hash2.Get("Ben") ||
		hash1.Get("Bob") != hash2.Get("Bob") ||
		hash1.Get("Bonny") != hash2.Get("Bonny") {
		t.Errorf("Direct matches should always return the same entry")
	}
	if hash1.Get("Ben") != hash2.GetReplicated("Ben", 1)[0] ||
		hash1.Get("Bob") != hash2.GetReplicated("Bob", 1)[0] ||
		hash1.Get("Bonny") != hash2.GetReplicated("Bonny", 1)[0] {
		t.Errorf("Direct matches should always return the same entry with GetReplicated")
	}

}

func TestGetReplicatedHappyPath(t *testing.T) {
	hr := New(20, nil)
	hr.Add("Bill", "Bob", "Bonny", "Clyde", "Computer", "Long")

	for _, itbl := range []struct {
		replicas int
		expKeys  int
	}{
		{replicas: 4, expKeys: 4},
		{replicas: 5, expKeys: 5},
		{replicas: 6, expKeys: 6},
		{replicas: 7, expKeys: 6},
		{replicas: 8, expKeys: 6},
		{replicas: 9, expKeys: 6},
		{replicas: 10, expKeys: 6},
		{replicas: 11, expKeys: 6},
		{replicas: 12, expKeys: 6},
		{replicas: 13, expKeys: 6},
		{replicas: 14, expKeys: 6},
	} {
		tbl := itbl
		t.Run(fmt.Sprintf("repl%d", tbl.replicas), func(t *testing.T) {
			oneHash := hr.Get("zombie")
			multiHash := hr.GetReplicated("zombie", tbl.replicas)
			if len(multiHash) != tbl.expKeys {
				t.Fatalf("unexpected length of return from GetReplicated: %d (expected %d)",
					len(multiHash), tbl.expKeys)
			}
			if multiHash[0] != oneHash {
				t.Errorf("element zero from GetReplicated should match Get; got %q and %q respectively",
					multiHash[0], oneHash)
			}
			for k := range hr.keys {
				if hr.Get(k) != hr.GetReplicated(k, 1)[0] {
					t.Errorf("Direct matches should always return the same entry with GetReplicated")
				}
				// Check that a manually constructed key gets the same hash as a
				// computed one (this way we have a test ensuring stability across
				// versions)
				if gdo, gro := hr.idxedKeyReplica(string(append([]byte(k), 0xaa, 0xaa, 0x01)), 0), hr.idxedKeyReplica(k, 1); gdo != gro {
					t.Errorf("mismatched second object-hashes for %q direct() = %d; keyed() = %d", k, gdo, gro)
				}
			}
		})
	}
}

func TestGetReplicatedChosenHashes(t *testing.T) {
	type keyReplID struct {
		k string
		r int
	}
	for _, itbl := range []struct {
		name       string
		baseHashes map[string]uint32
		replHashes map[keyReplID]uint32
		segsPerKey int // most cases will set this to 1 (must be < 128 for test-purposes)
		ownerKeys  []string
		// map from key to expected owners
		// inner value is ordered (by replica-number)
		expKeyOwners map[string][]string
		qReplicas    int // must be >1 to do anything interesting
	}{
		{
			name:       "simple_2_replicas_no_collision",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"fizzle": 5, // put fizzle in apple's range
			},
			qReplicas: 2,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 35,
			},
			expKeyOwners: map[string][]string{
				"fizzle": {"apple", "strawberry"},
			},
		},
		{
			name:       "2_replicas_collision",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"fizzle": 5, // put fizzle in apple's range
			},
			qReplicas: 2,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 9,
			},
			expKeyOwners: map[string][]string{
				"fizzle": {"apple", "orange"},
			},
		},
		{
			name:       "2_replicas_collision_exact",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"fizzle": 5, // put fizzle in apple's range
			},
			qReplicas: 2,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 10,
			},
			expKeyOwners: map[string][]string{
				"fizzle": {"apple", "orange"},
			},
		},
		{
			name:       "3_replicas_collision_exact",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"fizzle": 5, // put fizzle in apple's range
			},
			qReplicas: 3,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 10,
				{k: "fizzle", r: 2}: 20,
			},
			expKeyOwners: map[string][]string{
				"fizzle": {"apple", "orange", "pear"},
			},
		},
		{
			name:       "3_replicas_wrap_around",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"fizzle": 5, // put fizzle in apple's range
			},
			qReplicas: 3,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 10,
				{k: "fizzle", r: 2}: 0xffff, // much larger than "0plum"
			},
			expKeyOwners: map[string][]string{
				"fizzle": {"apple", "orange", "pear"},
			},
		},
		{
			name:       "3_replicas_wrap_around_segs_per_key_2",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 2,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"1apple": 110, "1orange": 120,
				"1pear": 130, "1strawberry": 140,
				"1peach": 150, "1plum": 160,
				"fizzle": 15, // put fizzle in orange's range
			},
			qReplicas: 3,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 55,
				{k: "fizzle", r: 2}: 160, // equal to 1plum
			},
			expKeyOwners: map[string][]string{
				"fizzle": {"orange", "plum", "apple"},
			},
		},
		{
			name:       "3_replicas_wrap_around_collision",
			ownerKeys:  []string{"apple", "orange", "pear", "strawberry", "peach", "plum"},
			segsPerKey: 1,
			baseHashes: map[string]uint32{
				"0apple": 10, "0orange": 20,
				"0pear": 30, "0strawberry": 40,
				"0peach": 50, "0plum": 60,
				"fizzle": 5,  // put fizzle in apple's range
				"boop":   51, // put boop in plum's range
			},
			qReplicas: 3,
			replHashes: map[keyReplID]uint32{
				{k: "fizzle", r: 1}: 55,
				{k: "fizzle", r: 2}: 60,
				{k: "boop", r: 1}:   55,
				{k: "boop", r: 2}:   60,
			},
			expKeyOwners: map[string][]string{
				"fizzle": {"apple", "plum", "orange"},
				"boop":   {"plum", "apple", "orange"},
			},
		},
	} {
		tbl := itbl
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
			for k, expOwners := range tbl.expKeyOwners {
				owners := hr.GetReplicated(k, tbl.qReplicas)
				if len(owners) != len(expOwners) {
					t.Errorf("unexpected number of owners returned: got %d; want %d\n got: %v\nwant: %v",
						len(owners), len(expOwners), owners, expOwners)
					continue
				}
				for i, owner := range owners {
					expOwner := expOwners[i]
					if owner != expOwner {
						t.Errorf("owner at index %d does not match expectation:\n got: %q\nwant: %q",
							i, owner, expOwner)
					}
				}
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	for _, itbl := range []struct {
		segsPerKey int
		shards     int
	}{
		{segsPerKey: 50, shards: 8},
		{segsPerKey: 50, shards: 32},
		{segsPerKey: 50, shards: 128},
		{segsPerKey: 50, shards: 512},
	} {
		tbl := itbl
		b.Run(fmt.Sprintf("segs%d-shards%d", tbl.segsPerKey, tbl.shards), func(b *testing.B) {

			hash := New(tbl.segsPerKey, nil)

			var buckets []string
			for i := 0; i < tbl.shards; i++ {
				buckets = append(buckets, fmt.Sprintf("shard-%d", i))
			}

			hash.Add(buckets...)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				hash.Get(buckets[i&(tbl.shards-1)])
			}
		})
	}
}

func BenchmarkGetReplicated(b *testing.B) {
	for _, itbl := range []struct {
		segsPerKey int
		shards     int
		replicas   int
	}{
		{segsPerKey: 50, shards: 8, replicas: 2},
		{segsPerKey: 50, shards: 32, replicas: 2},
		{segsPerKey: 50, shards: 128, replicas: 2},
		{segsPerKey: 50, shards: 512, replicas: 2},
		{segsPerKey: 50, shards: 8, replicas: 4},
		{segsPerKey: 50, shards: 32, replicas: 4},
		{segsPerKey: 50, shards: 128, replicas: 4},
		{segsPerKey: 50, shards: 512, replicas: 4},
		{segsPerKey: 50, shards: 8, replicas: 8},
		{segsPerKey: 50, shards: 32, replicas: 8},
		{segsPerKey: 50, shards: 128, replicas: 8},
		{segsPerKey: 50, shards: 512, replicas: 8},
	} {
		tbl := itbl
		b.Run(fmt.Sprintf("segs%d-shards%d-reps%d", tbl.segsPerKey, tbl.shards, tbl.replicas), func(b *testing.B) {
			b.ReportAllocs()

			hash := New(tbl.segsPerKey, nil)

			var buckets []string
			for i := 0; i < tbl.shards; i++ {
				buckets = append(buckets, fmt.Sprintf("shard-%d", i))
			}

			hash.Add(buckets...)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				hash.GetReplicated(buckets[i&(tbl.shards-1)], tbl.replicas)
			}
		})
	}
}
