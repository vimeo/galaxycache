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
	"strings"
	"testing"
)

func FuzzDecString(f *testing.F) {
	// some valid inputs
	f.Add(string(byte(36)) + randID() + "\000")
	f.Add("\003abc\000")
	// a few invalid inputs
	f.Add("")                                   // empty
	f.Add(string(byte(12)) + randID() + "\000") // length too small
	f.Add("\027abc\000")                        // length too large
	f.Add("a")                                  // also, technically a length that's too small
	f.Add(randID())                             // no prefix, so probably too small, but there's a small chance of it being a length that's right, or small
	f.Fuzz(func(t *testing.T, in string) {
		ConsumeString([]byte(in))
	})
}

func TestConsumeStringInvalid(t *testing.T) {
	for _, tbl := range []struct {
		name         string
		in           string
		expErrSubStr string
	}{
		{
			name:         "incorrect_length_only",
			in:           "\003",
			expErrSubStr: "length prefix encodes length longer than remaining buffer",
		},
		{
			name:         "no_null_separator",
			in:           "\003aaaaaaaaaaaaa",
			expErrSubStr: "missing null separator or incorrect string length",
		},
		{
			name:         "overflow_uint64_length",
			in:           "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
			expErrSubStr: "varint length is too large for a uint64",
		},
	} {
		t.Run(tbl.name, func(t *testing.T) {
			b := []byte(tbl.in)
			s, ob, outErr := ConsumeString(b)
			if outErr == nil {
				t.Fatalf("invalid input %q succeeded decoding: got %q; remaining: %q", tbl.in, s, ob)
			}
			outErrStr := outErr.Error()
			if !strings.Contains(outErrStr, tbl.expErrSubStr) {
				t.Errorf("error missing expected substring %q; error: %q", tbl.expErrSubStr, outErrStr)
			}
		})
	}
}

func TestConsumeIntInvalid(t *testing.T) {
	for _, tbl := range []struct {
		name         string
		in           string
		expErrSubStr string
	}{
		{
			name:         "missing_trailing_null_separator_too_short",
			in:           "\003",
			expErrSubStr: "consumed entire string",
		},
		{
			name:         "no_null_separator",
			in:           "\003aaaaaaaaaaaaa",
			expErrSubStr: "missing null separator",
		},
		{
			name:         "overflow_uint64_length",
			in:           "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
			expErrSubStr: "varint ID is too large for a uint64",
		},
		{
			name:         "incomplete_varint",
			in:           "\xff\xff\xff",
			expErrSubStr: "buffer too small for varint decoding",
		},
	} {
		t.Run(tbl.name, func(t *testing.T) {
			b := []byte(tbl.in)
			s, ob, outErr := ConsumeInt(b)
			if outErr == nil {
				t.Fatalf("invalid input %q succeeded decoding: got %q; remaining: %q", tbl.in, s, ob)
			}
			outErrStr := outErr.Error()
			if !strings.Contains(outErrStr, tbl.expErrSubStr) {
				t.Errorf("error missing expected substring %q; error: %q", tbl.expErrSubStr, outErrStr)
			}
		})
	}
}
