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

package jsoncodec

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/vimeo/galaxycache"
)

// BackendGetter is an adapter that implements galaxycache.BackendGetter
// (it wraps an unexported type because type-inference is much better on function-calls)
func BackendGetter[T any](f func(ctx context.Context, key string) (T, error)) galaxycache.BackendGetter {
	return backendGetter[T](f)
}

// backendGetter is an adapter type that implements galaxycache.BackendGetter
type backendGetter[T any] func(ctx context.Context, key string) (T, error)

// Get populates dest with the value identified by key
// The returned data must be unversioned. That is, key must
// uniquely describe the loaded data, without an implicit
// current time, and without relying on cache expiration
// mechanisms.
func (b backendGetter[T]) Get(ctx context.Context, key string, dest galaxycache.Codec) error {
	out, bgErr := b(ctx, key)
	if bgErr != nil {
		return bgErr
	}
	if d, ok := dest.(*Codec[T]); ok {
		d.Set(out)
		return nil
	}
	return setSlow(out, dest)
}

func setSlow[T any](out T, dest galaxycache.Codec) error {
	vs, mErr := json.Marshal(out)
	if mErr != nil {
		return fmt.Errorf("failed to marshal value as bytes: %w", mErr)
	}

	if uErr := dest.UnmarshalBinary(vs); uErr != nil {
		return fmt.Errorf("destination codec (type %T) Unmarshal failed: %w", dest, uErr)
	}
	return nil
}

// BackendGetterWithInfo is an adapter that implements galaxycache.BackendGetter
// (it wraps an unexported type because type-inference is much better on function-calls)
func BackendGetterWithInfo[T any](f func(ctx context.Context, key string) (T, galaxycache.BackendGetInfo, error)) galaxycache.BackendGetterWithInfo {
	return backendGetterWithInfo[T](f)
}

// backendGetter is an adapter type that implements galaxycache.BackendGetter
type backendGetterWithInfo[T any] func(ctx context.Context, key string) (T, galaxycache.BackendGetInfo, error)

// GetWithInfo populates dest with the value identified by `key`
// The returned data must be unversioned. That is, `key` must
// uniquely describe the loaded data, without an implicit
// current time, and without relying on cache expiration
// mechanisms.
func (b backendGetterWithInfo[T]) GetWithInfo(ctx context.Context, key string, dest galaxycache.Codec) (galaxycache.BackendGetInfo, error) {
	out, bgInfo, bgErr := b(ctx, key)
	if bgErr != nil {
		return galaxycache.BackendGetInfo{}, bgErr
	}
	if d, ok := dest.(*Codec[T]); ok {
		d.Set(out)
		return bgInfo, nil
	}
	return bgInfo, setSlow(out, dest)
}
