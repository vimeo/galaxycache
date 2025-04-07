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

// Package jsoncodec provides some helpful wrappers and a core [Codec]
// implementation to make using JSON payloads easy within systems using
// Galaxycache.
//
// In general, it is recommended to use the
// [github.com/vimeo/galaxycache/protocodec.CodecV2] codec if at all possible,
// as protobuf generally provides lower overhead in both space and
// proccessing-time, schemas that can evolve, and the ability to rename fields
// as necessary.
//
// This package should *_not_* be used for caching protobuf message-types, it is
// *_always_* better to use binary marshaling with
// [github.com/vimeo/galaxycache/protocodec.CodecV2] in that case.
package jsoncodec

import (
	"encoding/json"
)

// Codec wraps another type, providing a simple wrapper for clients that want to wrap JSON.
//
// Note: protocodec should generally be prefered for new deployments. This
// implementation is provided to provide an easy way to insert Galaxycache into
// systems that are already using JSON internally for the payloads they'll be working with.
type Codec[T any] struct {
	msg T
}

// MarshalBinary on a ProtoCodec returns the encoded proto message
func (c *Codec[T]) MarshalBinary() ([]byte, error) {
	return json.Marshal(&c.msg)
}

// UnmarshalBinary on a ProtoCodec unmarshals provided data into
// the proto message
func (c *Codec[T]) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, &c.msg)
}

// Get implies returns the internal protobuf message value
func (c *Codec[T]) Get() *T {
	return &c.msg
}

// Set bypasses Marshaling and lets you set the internal protobuf value
func (c *Codec[T]) Set(v T) {
	c.msg = v
}
