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

	t.Log(hash1.hashMap[uint32('0')], hash2.hashMap[uint32('0')])
	t.Logf("%+v", hash1.hashMap)
	t.Logf("%v", hash1.keyHashes)
	t.Logf("%+v", hash2.hashMap)
	t.Logf("%v", hash2.keyHashes)
	if hash1.hashMap[uint32('0')] != hash2.hashMap[uint32('0')] {
		t.Errorf("inconsistent owner for hash %d: %s vs %s", 'B',
			hash1.hashMap[uint32('B')], hash2.hashMap[uint32('B')])
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

func TestGetReplicated(t *testing.T) {
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
