//go:build go1.23

// Package chtest provides some helper hash-functions for use with the parent
// consistenthash package (and galaxycache) to provide particular owners for
// specific keys.
//
// This package cannot be imported outside of tests.
// If [testing.Testing] returns false, it will panic in init() unless compiled
// with the `testing_binary_chtest_can_panic_at_any_time` build tag (intended
// to be used for tests that require running a separate binary
// (e.g. integration/interop tests)).
package chtest

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/vimeo/galaxycache/cachekey"
	"github.com/vimeo/galaxycache/consistenthash"
)

const singleOwnerPrefix = "Single Key Prefix\000\000"
const fallthroughOwnerKeyPrefix = "Fallthrough Key Prefix\000\000"

// SingleOwnerKey returns a key that is guaranteed to map to that owner (as
// long as that owner was included in the call to [NewMapArgs]
func SingleOwnerKey(owner string) string {
	return singleOwnerPrefix + owner
}

// FallthroughKey constructs a key that is owned by original owner, and if
// origOwner is removed from the hash-ring, falls through to fallthroughOwner.
// ... as long as both owners were included in the call to [NewMapArgs]
func FallthroughKey(origOwner, fallthroughOwner string) string {
	outBuf := []byte{}
	outBuf = append(outBuf, fallthroughOwnerKeyPrefix...)
	outBuf = cachekey.AppendStrID(outBuf, origOwner)
	return string(cachekey.AppendStrID(outBuf, fallthroughOwner))
}

type hashRange struct {
	lo, hi uint32
}

// Args is an argument struct to NewMapArgs
type Args struct {
	Owners []string
	// Map from a key to a set of replicas to the set of owners to map this key into for its sequential replicas (GetReplicated)
	// if the value is nil, then one hash for each owner is picked sequentially
	RegisterKeys map[string][]string
}

// MapArgs provides the values that should be passed through to
// [consistenthash.NewMap] (possibly indirectly)
type MapArgs struct {
	NSegsPerKey int
	HashFunc    func([]byte) uint32
}

// NewMap creates a new [consistenthash.Map] with the args from the receiver.
// It does _not_ populate the hashring.
func (m *MapArgs) NewMap() *consistenthash.Map {
	return consistenthash.New(m.NSegsPerKey, m.HashFunc)
}

// NewMapArgs creates a [consistenthash.Map] that uses a constructed "hash function" that is engineered to pick the correct, the integer
// it computes required value of segsPerKey to make every ordering of
// the entries in owners appear in the hashring.
func NewMapArgs(args Args) MapArgs {

	///////////////////////////////////////////////////////////////////////////
	// Here's how this works:
	// We not only need segments owned by each individual owner, but we
	// need to arrange for segments that fallthrough to each other owner.
	// To make this possible, for each pair of owners, we insert 4 segments to cover both orderings.
	// For the n=2 case, this will look like (4 segments):
	// |    a    |      b     |       b       |      a      |
	// Note: we could make a pass that coalesces this into 2 segments
	// instead of 4, but it's not worth the complexity in a test package.
	//
	// As a result, although we have  n * (n-1) pairs, we need another factor of 2.
	//
	// Similarly, the n = 3 case looks more complicated (12 segments):
	// | a | b | a | c | b | a | b | c | c | a | c | b |
	//
	// We try to distribute the segments between 0 and 2^31 to make things
	// robust, and have space for everything without having to deal with
	// signed/unsigned conversion issues or overflows.
	//
	// For Fallthrough keys, we pick the lowest value for the segment that
	// we created for that specific fallthrough.
	// For Single-owner keys, we just pick the first segment for that owner.
	// Pre-registered keys work the same as single-owner keys, but are
	// stored in a separate map.
	//
	// The keen-eyed observer will note that there are some segments that
	// can be merged/dedup'd in the n=3 case as well, but as this is a
	// test-helper package, simplicity is the most important, so it isn't
	// worth complicating this code with any merging/simplification logic.
	//////////////////////////////////////////////////////////////////////////

	owners := args.Owners
	ownersMap := make(map[string][]hashRange, len(owners))
	segsPerKey := (len(owners) - 1) * 2 // we need to cover both orderings for each pair
	nSegs := segsPerKey * len(owners)
	type indexedString struct {
		idx int
		str string
	}
	// This doesn't need to be particularly efficient, just use a map to make sure we get the key indices right.
	keyIdx := map[string]int{}
	// we need both orderings, so we can't divide by two
	ownerPairs := func(yield func(indexedString, indexedString) bool) {
		for i, left := range owners {
			for j, right := range owners {
				if i == j {
					// skip the left == right case; it's uninteresting (outside the consistenthash package itself)
					continue
				}
				lIdx := keyIdx[left]
				rIdx := keyIdx[right]
				// Make sure to increment _after_ getting the current index.
				keyIdx[left]++
				keyIdx[right]++
				l := indexedString{lIdx, left}
				r := indexedString{rIdx, right}
				if !yield(l, r) {
					return
				}
			}
		}
	}

	type lrPair struct {
		l, r string
	}
	fallthroughs := make(map[lrPair]hashRange, nSegs)

	ownerKeyHashes := make(map[string]uint32, nSegs)
	// Use the signed max so we don't have to be exact.
	rangeSize := uint32(math.MaxInt32 / nSegs)
	lastUpper := uint32(0)
	for left, right := range ownerPairs {
		leftUpperBound := lastUpper + rangeSize
		rightUpperBound := leftUpperBound + rangeSize
		leftKey := strconv.Itoa(left.idx) + left.str
		rightKey := strconv.Itoa(right.idx) + right.str
		ownerKeyHashes[leftKey] = leftUpperBound
		ownerKeyHashes[rightKey] = rightUpperBound
		lRange := hashRange{lastUpper + 1, leftUpperBound}
		ownersMap[left.str] = append(ownersMap[left.str], lRange)
		ownersMap[right.str] = append(ownersMap[right.str], hashRange{leftUpperBound + 1, rightUpperBound})
		fallthroughs[lrPair{left.str, right.str}] = lRange

		lastUpper = rightUpperBound
	}

	preRegisteredKeys := map[string]uint32{}
	for k, kHosts := range args.RegisterKeys {
		if kHosts == nil {
			kHosts = owners
		}
		for i, kHost := range kHosts {
			// if i == 0, we use the key verbatim
			// This _must_ be kept in sync with Map.idxedKeyReplica
			regKey := k
			if i > 0 {
				regKey = string(binary.AppendUvarint([]byte(k+"\xaa\xaa"), uint64(i)))
			}
			preRegisteredKeys[regKey] = ownersMap[kHost][0].hi
		}
	}

	h := func(buf []byte) uint32 {
		bufStr := string(buf)
		if hVal, ok := ownerKeyHashes[bufStr]; ok {
			return hVal
		}
		if owner, ok := strings.CutPrefix(bufStr, singleOwnerPrefix); ok {
			return ownersMap[owner][0].hi // this will panic if the owner isn't known, but that's fine
		}
		if owners, ok := strings.CutPrefix(bufStr, fallthroughOwnerKeyPrefix); ok {
			origOwner, ftOwners, origOwnerErr := cachekey.ConsumeString([]byte(owners))
			if origOwnerErr != nil {
				panic(fmt.Errorf("invalid fallthrough key; failed to extract original owner: %w",
					origOwnerErr))
			}
			ftOwner, emptyStr, fallthroughErr := cachekey.ConsumeString(ftOwners)
			if fallthroughErr != nil {
				panic(fmt.Errorf("invalid fallthrough key; failed to extract fallthrough owner: %w",
					fallthroughErr))
			}
			// We can remove this if allowing arbitrary suffixes is useful enough
			if len(emptyStr) != 0 {
				panic("unconsumed component")
			}
			return fallthroughs[lrPair{origOwner, ftOwner}].hi
		}
		if hv, ok := preRegisteredKeys[bufStr]; ok {
			return hv
		}
		panic(fmt.Errorf("unknown key: %q", string(buf)))
	}

	return MapArgs{
		NSegsPerKey: segsPerKey,
		HashFunc:    h,
	}
}
