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
	"runtime"
	"testing"
	"time"

	"github.com/vimeo/go-clocks/fake"
)

func TestTypedGetWithExpiry(t *testing.T) {
	startTime := time.Now()

	getTests := []struct {
		name       string
		keyToAdd   string
		keyToGet   string
		keyExpiry  time.Time
		setTime    time.Time
		expectedOK bool
	}{
		{name: "string_hit_zero_val_no_advance", keyToAdd: "myKey", keyToGet: "myKey", setTime: startTime, expectedOK: true},
		{name: "string_miss_zero_val_no_advance", keyToAdd: "myKey", keyToGet: "nonsense", setTime: startTime, expectedOK: false},
		{name: "string_hit_zero_val_big_jump", keyToAdd: "myKey", keyToGet: "myKey", setTime: startTime.Add(time.Hour * 24 * 365), expectedOK: true},
		{name: "string_miss_zero_val_big_jump", keyToAdd: "myKey", keyToGet: "nonsense", setTime: startTime.Add(time.Hour * 24 * 365), expectedOK: false},
		{name: "string_hit_expiring_big_jump", keyToAdd: "myKey", keyToGet: "myKey", keyExpiry: startTime.Add(time.Hour), setTime: startTime.Add(time.Hour * 24 * 365), expectedOK: false},
		{name: "string_hit_expiring_exact_jump", keyToAdd: "myKey", keyToGet: "myKey", keyExpiry: startTime.Add(time.Hour), setTime: startTime.Add(time.Hour), expectedOK: false},
		{name: "string_hit_expiring_jump_just_under", keyToAdd: "myKey", keyToGet: "myKey", keyExpiry: startTime.Add(time.Hour), setTime: startTime.Add(time.Hour - time.Nanosecond), expectedOK: true},
	}

	for _, tt := range getTests {
		t.Run(tt.name, func(t *testing.T) {
			clk := fake.NewClock(startTime)
			lru := TypedNew[string, int](0)
			lru.Clock = clk
			lru.AddExpiring(tt.keyToAdd, 1234, tt.keyExpiry)

			clk.SetClock(tt.setTime)

			val, exp, ok := lru.GetWithExpiry(tt.keyToGet)
			if ok != tt.expectedOK {
				t.Fatalf("%s: cache hit = %v; want %v", tt.name, ok, !ok)
			} else if ok {
				if val != 1234 {
					t.Fatalf("%s expected get to return 1234 but got %v", tt.name, val)
				}
				if !exp.Equal(tt.keyExpiry) {
					t.Errorf("%s expected expiration time to match set keyExpiry: got %s; want %s", tt.name, exp, tt.keyExpiry)
				}
			}
		})
	}
}

func TestTypedEvictExpire(t *testing.T) {
	evictedKeys := make([]string, 0)
	startTime := time.Now()
	clk := fake.NewClock(startTime)

	lru := TypedNew[string, int](20)
	lru.Clock = clk
	onEvictedFun := func(key string, value int) {
		// 0 should have been evicted before we hit capacity (during the AddExpiring call for i=1, before it's inserted)
		if key == "myKey0" && lru.Len() != 0 {
			t.Errorf("myKey0 evicted due to capacity, not expiry (lru size %d)", lru.Len())
		}
		evictedKeys = append(evictedKeys, key)
	}
	lru.OnEvicted = onEvictedFun
	for i := range 22 {
		lru.AddExpiring(fmt.Sprintf("myKey%d", i), 1234, startTime.Add(time.Minute*time.Duration(i)))
	}

	if len(evictedKeys) != 2 {
		t.Logf("lru contents: %v; lru: %+v", lru.cache, lru)
		t.Fatalf("got %d evicted keys; want 2 (len %d)", len(evictedKeys), lru.Len())
	}
	if evictedKeys[0] != "myKey0" {
		t.Fatalf("got %v in first evicted key; want %s", evictedKeys[0], "myKey0")
	}
	if evictedKeys[1] != "myKey1" {
		t.Fatalf("got %v in second evicted key; want %s", evictedKeys[1], "myKey1")
	}
	// move 9 and 10 to the head
	lru.Get("myKey10")
	lru.Get("myKey9")

	// advance 4 minutes so keys 0-4 get evicted on the next add
	clk.Advance(time.Minute * 4)
	lru.Add(fmt.Sprintf("myKey%d", 22), 1234)
	if lru.Len() != 18 {
		t.Errorf("post-time advance; unexpected LRU size: got %d; want %d", lru.Len(), 18)
	}
	// add another few keys to evict the the others
	for i := 23; i < 32; i++ {
		lru.Add(fmt.Sprintf("myKey%d", i), 1234)
	}
	// Force a gc and add another key so the weak pointers for evicted
	// items will generally return `nil` when we try to promote them.
	runtime.GC()
	lru.Add("myKey33", 1234)
}
