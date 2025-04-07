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
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
	"time"
)

// EpochConfig defines the tunables behind how epoch-numbers are calculated.
// In particular, it encapsulates the reference time, epoch length, and an
// integer ID jitter coefficient (so small integer IDs get spread out sufficiently).
//
// This implements the scheme described in the [Vimeo Engineering Blog post entitled "Vimeo metadata caching at Vimeo"](https://medium.com/vimeo-engineering-blog/video-metadata-caching-at-vimeo-a54b25f0b304).
// In particular, it provides a way to implement TTLs without explicit support
// from Galaxycache by rotating the cache-key every [EpochConfig.EpochLength],
// and offsetting that by a bit based on the input ID, we prevent individual
// keys from expiring/rotating at exactly the same time.
//
// Note: for this to be useful during rollouts, these parameters should be
// stable over time, so we recommend defining constants in your code for the
// two time.Duration fields.
type EpochConfig struct {
	// Base timestamp from which epochs are measured. By making a
	// relatively recent timestamp that still guarantees that the epoch is
	// positive, we can save a couple bytes in our keys.
	EpochZero time.Time

	// A prime number of nanoseconds indicating the length of an epoch.
	// For example, one can use 396_999_999_953 for a ~6.5-ish minute epoch
	// that's about 23s less than 7 minutes.
	// note that 23, 7 and 397 are all primes, so the epochs will wrap around nicely over time.
	EpochLength time.Duration

	// A large prime to multiply integer IDs by to ensure that adjacent IDs get spread out.
	// For example, one might use 330_999_999_991 to work with the 7 minute-ish value above
	// it would be chosen to be about 5 minutes 31 seconds in nanoseconds.
	// 331 is a prime number of seconds, as are 5 minutes and 31 seconds.
	// Due to constraints on the bits.Div64 function, we can't have too
	// large a value in hi, so this multiplier is required to be strictly
	// less than epochLength if we want to support the full uint64 space for integer IDs.
	// This should give a good spread of jitter values from epochMode.
	IntIDJitterPrimeMultiplier time.Duration
}

// Jitter is an int128 opaque value used with AppendEpoch, and EpochConfig to
// track the epoch boundary offset for a specific ID.
type Jitter struct {
	hi, lo uint64
}

func (j Jitter) epochMod(epochLength uint64) uint64 {
	// Take the jitter mod the epoch length so we only jitter within a epoch-window
	_, jMod := bits.Div64(j.hi, j.lo, epochLength)
	return jMod
}

// JitterStrID hashes the ID and computes a jittered value using a truncated sha256.
func JitterStrID[I ~string](id I) Jitter {
	h := sha256.Sum256([]byte(id))
	// Use 4 bytes from hi, so we don't overflow our result when
	// doing division/modulus operations later
	// bits.Div64 panics in epochMod panics if hi is epochLength <= hi (quotient overflow).
	hi := binary.BigEndian.Uint32(h[:4])
	lo := binary.BigEndian.Uint64(h[4:12])
	return Jitter{
		hi: uint64(hi),
		lo: lo,
	}
}

// JitterIntID returns a jittered offset within the ID
func JitterIntID[I ~uint64](e *EpochConfig, id I) Jitter {
	hi, lo := bits.Mul64(uint64(id), uint64(e.IntIDJitterPrimeMultiplier.Nanoseconds()))
	return Jitter{hi: hi, lo: lo}
}

func computeEpoch(cfg EpochConfig, now time.Time, jitter Jitter) uint64 {
	offset := now.Sub(cfg.EpochZero)
	jitteredOffset := offset + time.Duration(jitter.epochMod(uint64(cfg.EpochLength.Nanoseconds())))/time.Nanosecond
	return uint64(jitteredOffset / cfg.EpochLength)
}

// AppendEpoch appends the jittered epoch suffix.
// Jitter is taken mod the epoch config's EpochLength (in nanoseconds)
// The epoch number is encoded with AppendUint
// See the comment on [EpochConfig] for information about how to use this tool.
func AppendEpoch(buf []byte, cfg EpochConfig, now time.Time, jitter Jitter) []byte {
	epoch := computeEpoch(cfg, now, jitter)
	return AppendUint(buf, epoch)
}
