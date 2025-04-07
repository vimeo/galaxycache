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
	"encoding/binary"
	"errors"
	"fmt"
)

// ConsumeString returns a consumed string and the remaining buffer or an error if the input is malformed
func ConsumeString(buf []byte) (string, []byte, error) {
	sz, consumed := binary.Uvarint(buf)
	if consumed == 0 {
		return "", nil, errors.New("buffer too small for varint decoding of length prefix")
	} else if consumed < 0 {
		return "", nil, fmt.Errorf("varint length is too large for a uint64: encoding is %d bytes", -consumed)
	} else if sz >= uint64(len(buf)-consumed) {
		return "", nil, fmt.Errorf("length prefix encodes length longer than remaining buffer (%d > %d)",
			sz, uint64(len(buf)-consumed))
	}
	buf = buf[consumed:]
	out := string(buf[:sz])
	buf = buf[sz:]
	if buf[0] != '\000' {
		return "", nil, errors.New("missing null separator or incorrect string length")
	}
	return out, buf[1:], nil
}

// ConsumeInt returns a consumed int and the remaining buffer or an error if the input is malformed
func ConsumeInt(buf []byte) (uint64, []byte, error) {
	sz, consumed := binary.Uvarint(buf)
	if consumed == 0 {
		return 0, nil, errors.New("buffer too small for varint decoding")
	} else if consumed < 0 {
		return 0, nil, fmt.Errorf("varint ID is too large for a uint64: encoding is %d bytes", -consumed)
	}
	buf = buf[consumed:]
	if len(buf) < 1 {
		return 0, nil, fmt.Errorf("consumed entire string (%d bytes), with no space for null separator", consumed)
	}
	if buf[0] != '\000' {
		return 0, nil, errors.New("missing null separator")
	}
	return sz, buf[1:], nil
}
