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

// Package cachekey defines several helper functions for building up and decoding
// keys for use by clients of galaxycache.
//
// The encoding scheme has three key design criteria: reasonable performance, simplicity and robustness
//
//   - All fields are separated by null bytes
//   - Strings are prefixed with a varint length (as handled by encoding/binary)
//   - Integers are encoded as a varint.
//
// This design is inspired by
// [OrderedCodes](https://github.com/google/orderedcode), but, since keys do
// not need to preserve ordering, we can use a slightly simpler design.
package cachekey

import (
	"encoding/binary"
	"slices"
)

// AppendStrID encodes a string ID into a form that can be concatenated into a V2 galaxycache key
func AppendStrID[I ~string](buf []byte, id I) []byte {
	// The IDs are currently guaranteed to be less than 127 bytes long, so
	// we can assume that our varint length is exactly one byte.
	buf = slices.Grow(buf, 2+len(id))
	buf = binary.AppendUvarint(buf, uint64(len(id)))
	buf = append(buf, id...)
	// add the separating null-byte
	buf = append(buf, byte('\000'))
	return buf
}

// AppendUint encodes an unsigned integer into a form that can be concatenated into a V2 galaxycache key
func AppendUint[I ~uint64](buf []byte, id I) []byte {
	encBuf := [binary.MaxVarintLen64]byte{}
	n := binary.PutUvarint(encBuf[:], uint64(id))
	buf = slices.Grow(buf, n+1)
	buf = append(buf, encBuf[:n]...)
	// add the separating null-byte
	buf = append(buf, byte('\000'))
	return buf
}
