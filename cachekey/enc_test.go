//go:build go1.22

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

package cachekey

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

func randID() string {
	buf := [32]byte{}
	_, rErr := crand.Read(buf[:])
	if rErr != nil {
		panic(rErr)
	}
	return base32.StdEncoding.EncodeToString(buf[:])
}

func TestEncUUIDStringIDStackBuf(t *testing.T) {
	i := randID()
	buf := [128]byte{}
	b := AppendStrID(buf[:0], i)

	if len(b) != len(i)+2 {
		t.Fatalf("unexpected length for encoded string len = %d; expected %d", len(b), len(i)+2)
	}
	if b[len(b)-1] != '\000' {
		t.Errorf("unexpected final byte in encoded string: %q; expected %q", b[len(b)-1], '\000')
	}
	// varint encoding is base 128, so since UUIDs are much smaller (36
	// bytes), the first byte should be equivalent to a normal uint8
	// encoding. (the high bit is a continuation indicator)
	if b[0] != uint8(len(i)) {
		t.Errorf("unexpected first byte in encoded key %x; expected %x", b[0], uint8(len(i)))
	}
	if string(b[1:len(i)+1]) != i {
		t.Errorf("mis-encoded ID: got %q; expected %q", string(b[1:len(i)+1]), i)
	}

	if &b[0] != &buf[0] {
		t.Errorf("backing array reallocated: %p for stack array; %p for buffer", &buf[0], &b[0])
	}

	di, eb, decErr := ConsumeString(b)
	if decErr != nil {
		t.Fatalf("failed to decode single-ID string: %s", decErr)
	}
	if len(eb) != 0 {
		t.Errorf("failed to consume entire string: %d bytes remaining: %[2]q (%[2]x)", len(eb), eb)
	}
	if di != i {
		t.Errorf("round-trip failure: decoded ID %q doesn't match input %q", di, i)
	}
}

func TestEncDecUUIDStringIDnilBuf(t *testing.T) {
	i := randID()
	b := AppendStrID(nil, i)

	if len(b) != len(i)+2 {
		t.Fatalf("unexpected length for encoded string len = %d; expected %d", len(b), len(i)+2)
	}
	if b[len(b)-1] != '\000' {
		t.Errorf("unexpected final byte in encoded string: %q; expected %q", b[len(b)-1], '\000')
	}
	// varint encoding is base 128, so since UUIDs are much smaller (36
	// bytes), the first byte should be equivalent to a normal uint8
	// encoding. (the high bit is a continuation indicator)
	if b[0] != uint8(len(i)) {
		t.Errorf("unexpected first byte in encoded key %x; expected %x", b[0], uint8(len(i)))
	}
	if string(b[1:len(i)+1]) != i {
		t.Errorf("mis-encoded ID: got %q; expected %q", string(b[1:len(i)+1]), i)
	}
}

func TestEncIntIDStackBuf(t *testing.T) {
	i := uint64(rand.Int64())
	buf := [128]byte{}
	b := AppendUint(buf[:0], i)

	uvi := [binary.MaxVarintLen64]byte{}
	encLen := binary.PutUvarint(uvi[:], i)
	if len(b) != encLen+1 {
		t.Fatalf("unexpected length for encoded string len = %d; expected %d", len(b), encLen+1)
	}
	if b[len(b)-1] != '\000' {
		t.Errorf("unexpected final byte in encoded string: %q; expected %q", b[len(b)-1], '\000')
	}
	// varint encoding is base 128, so since UUIDs are much smaller (36
	// bytes), the first byte should be equivalent to a normal uint8
	// encoding. (the high bit is a continuation indicator)
	if !bytes.Equal(uvi[:encLen], b[:encLen]) {
		t.Errorf("unexpected encoded key %x; expected %x", b[:encLen], uvi[:encLen])
	}

	if &b[0] != &buf[0] {
		t.Errorf("backing array reallocated: %p for stack array; %p for buffer", &buf[0], &b[0])
	}

	di, eb, decErr := ConsumeInt(b)
	if decErr != nil {
		t.Fatalf("failed to decode single-ID string: %s", decErr)
	}
	if len(eb) != 0 {
		t.Errorf("failed to consume entire string: %d bytes remaining: %[2]q (%[2]x)", len(eb), eb)
	}
	if di != i {
		t.Errorf("round-trip failure: decoded ID %q doesn't match input %q", di, i)
	}
}

func TestEncIntIDnilBuf(t *testing.T) {
	i := uint64(rand.Int64())
	b := AppendUint(nil, i)

	uvi := [binary.MaxVarintLen64]byte{}
	encLen := binary.PutUvarint(uvi[:], i)
	if len(b) != encLen+1 {
		t.Fatalf("unexpected length for encoded string len = %d; expected %d", len(b), encLen+1)
	}
	if b[len(b)-1] != '\000' {
		t.Errorf("unexpected final byte in encoded string: %q; expected %q", b[len(b)-1], '\000')
	}
	// varint encoding is base 128, so since UUIDs are much smaller (36
	// bytes), the first byte should be equivalent to a normal uint8
	// encoding. (the high bit is a continuation indicator)
	if !bytes.Equal(uvi[:encLen], b[:encLen]) {
		t.Errorf("unexpected encoded key %x; expected %x", b[:encLen], uvi[:encLen])
	}
}

type testStringID string

// IsID is a noop implementation to make testStringID statisfy db.ID
func (t testStringID) IsID() {}

type testIntID uint64

// IsID is a noop implementation to make testIntID statisfy db.ID
func (t testIntID) IsID() {}

func TestJitterStrID(t *testing.T) {
	i := testStringID(randID())
	h := sha256.Sum256([]byte(i))
	high := binary.BigEndian.Uint32(h[:4])
	remaining := h[4:]
	low := binary.BigEndian.Uint64(remaining[:8])
	expJ := Jitter{
		hi: uint64(high),
		lo: low,
	}

	j := JitterStrID(i)
	if j != expJ {
		t.Errorf("unexpected string jitter for %q: expected %v; got %v", i, expJ, j)
	}

	secondI := testStringID(randID())
	if j == JitterStrID(secondI) {
		t.Errorf("jitter for distinct UUIDs (%q & %q) match: %v", i, secondI, j)
	}
	j.epochMod(396_999_999_953)
	sj := JitterStrID(secondI)
	sj.epochMod(396_999_999_953)
}

func TestJitterIntID(t *testing.T) {
	i := testIntID(rand.Int64())
	i2 := i + 5

	abs := func(z int64) int64 {
		if z >= 0 {
			return z
		}
		return -z
	}

	cfg := EpochConfig{
		EpochZero:                  time.Time{},
		EpochLength:                396_999_999_953,
		IntIDJitterPrimeMultiplier: 330_999_999_991,
	}
	el := uint64(cfg.EpochLength.Nanoseconds())

	j := JitterIntID(&cfg, i)
	expLow := uint64(cfg.IntIDJitterPrimeMultiplier) * uint64(i)
	if j.lo != expLow {
		t.Errorf("unexpected integer jitter for %d: expected lo: %d; got %v", i, expLow, j)
	}

	if j == JitterIntID(&cfg, i2) {
		t.Errorf("jitter for distinct integer IDs (%d & %d) match: %v", i, i2, j)
	}
	// make sure epochMod doesn't panic
	if diff := int64(j.epochMod(el)) - int64(JitterIntID(&cfg, i2).epochMod(el)); abs(diff) < 10_000_000 {
		t.Errorf("nearby clip IDs (%d and %d) have jitters separated by <10ms: %d & %d (diff: %d)",
			i, i2, j.epochMod(el), JitterIntID(&cfg, i2).epochMod(el), diff)
	}
}

func TestIntIDEpochDistribution(t *testing.T) {
	t.Parallel()

	cfg := EpochConfig{
		EpochZero:                  time.Time{},
		EpochLength:                396_999_999_953,
		IntIDJitterPrimeMultiplier: 330_999_999_991,
	}

	el := uint64(cfg.EpochLength.Nanoseconds())
	// modulus jitter into 10s buckets
	modBkts := make([]int, int(el/uint64(time.Second*10)+1))

	// take the Monte Carlo strategy of picking a number of random IDs

	const maxClipID = 1_000_000_000

	// take a 1% sample by default
	numIDs := 10_000_000
	fairMult := 1.02
	if testing.Short() {
		// if we're running with -short, use fewer clips
		numIDs = 10_000
		fairMult = 2.0
	}

	for range numIDs {
		z := testIntID(rand.Int64N(maxClipID))
		j := JitterIntID(&cfg, z)
		jMod := j.epochMod(el)
		modBkts[int(jMod)/int(time.Second*10)]++
	}

	minBkt := math.MaxInt
	maxBkt := 0
	for i, bkt := range modBkts {
		if bkt > maxBkt {
			maxBkt = bkt
		}
		// the last bucket is always less full because the interval isn't an even multiple of 10 s
		if bkt < minBkt && i != len(modBkts)-1 {
			minBkt = bkt
		}
	}
	if float64(maxBkt) > fairMult*float64(minBkt) {
		t.Errorf("bucket distribution unfair: max %d; min %d (>%fx)",
			maxBkt, minBkt, fairMult)
	}
	t.Logf("bucket distribution: max %d; min %d (%f ratio)",
		maxBkt, minBkt, float64(maxBkt)/float64(minBkt))
}

func FuzzEncStringRoundTrip(f *testing.F) {
	f.Add(randID())
	f.Add("a")
	f.Add("")
	f.Fuzz(func(t *testing.T, in string) {
		b := AppendStrID(nil, in)
		if len(b) == 0 {
			t.Errorf("encoded %q to a zero-length value", in)
		}

		di, eb, decErr := ConsumeString(b)
		if decErr != nil {
			t.Fatalf("failed to decode single-ID string: %s", decErr)
		}
		if len(eb) != 0 {
			t.Errorf("failed to consume entire string: %d bytes remaining: %[2]q (%[2]x)", len(eb), eb)
		}
		if di != in {
			t.Errorf("round-trip failure: decoded ID %q doesn't match input %q", di, in)
		}
	})
}

func FuzzEncIntRoundTrip(f *testing.F) {
	f.Add(uint64(100))
	f.Add(uint64(1 << 32))
	f.Add(uint64(1 << 63))
	f.Add(uint64(1<<63 | 1<<32))
	f.Fuzz(func(t *testing.T, in uint64) {
		b := AppendUint(nil, in)
		if len(b) == 0 {
			t.Errorf("encoded %q to a zero-length value", in)
		}

		di, eb, decErr := ConsumeInt(b)
		if decErr != nil {
			t.Fatalf("failed to decode single-ID string: %s", decErr)
		}
		if len(eb) != 0 {
			t.Errorf("failed to consume entire string: %d bytes remaining: %[2]q (%[2]x)", len(eb), eb)
		}
		if di != in {
			t.Errorf("round-trip failure: decoded ID %d doesn't match input %d", di, in)
		}
	})
}
