//go:build go1.18

package protocodec

import (
	"context"
	"fmt"

	"github.com/vimeo/galaxycache"

	"google.golang.org/protobuf/proto"
)

// BackendGetterV2 is an adapter that implements galaxycache.BackendGetter
// (it wraps an unexported type because type-inference is much better on function-calls)
func BackendGetterV2[C any, T pointerMessage[C]](f func(ctx context.Context, key string) (T, error)) galaxycache.BackendGetter {
	return backendGetterV2[C, T](f)
}

// backendGetterV2 is an adapter type that implements galaxycache.BackendGetter
type backendGetterV2[C any, T pointerMessage[C]] func(ctx context.Context, key string) (T, error)

// Get populates dest with the value identified by key
// The returned data must be unversioned. That is, key must
// uniquely describe the loaded data, without an implicit
// current time, and without relying on cache expiration
// mechanisms.
func (b backendGetterV2[C, T]) Get(ctx context.Context, key string, dest galaxycache.Codec) error {
	out, bgErr := b(ctx, key)
	if bgErr != nil {
		return bgErr
	}
	if d, ok := dest.(*CodecV2[C, T]); ok {
		d.Set(out)
		return nil
	}
	return setSlowV2(out, dest)
}

func setSlowV2[C any, T pointerMessage[C]](out T, dest galaxycache.Codec) error {
	vs, mErr := proto.Marshal(out)
	if mErr != nil {
		return fmt.Errorf("failed to marshal value as bytes: %w", mErr)
	}

	if uErr := dest.UnmarshalBinary(vs); uErr != nil {
		return fmt.Errorf("destination codec (type %T) Unmarshal failed: %w", dest, uErr)
	}
	return nil
}

// BackendGetterWithInfo is an adapter that implements [galaxycache.BackendGetterWithInfo]
// (it wraps an unexported type because type-inference is much better on function-calls)
func BackendGetterWithInfo[C any, T pointerMessage[C]](f func(ctx context.Context, key string) (T, galaxycache.BackendGetInfo, error)) galaxycache.BackendGetterWithInfo {
	return backendGetterWithInfo[C, T](f)
}

// backendGetterWithInfo is an adapter type that implements galaxycache.BackendGetterWithInfo
type backendGetterWithInfo[C any, T pointerMessage[C]] func(ctx context.Context, key string) (T, galaxycache.BackendGetInfo, error)

// GetWithInfo populates dest with the value identified by `key`
// The returned data must be unversioned. That is, `key` must
// uniquely describe the loaded data, without an implicit
// current time, and without relying on cache expiration
// mechanisms.
func (b backendGetterWithInfo[C, T]) GetWithInfo(ctx context.Context, key string, dest galaxycache.Codec) (galaxycache.BackendGetInfo, error) {
	out, bgInfo, bgErr := b(ctx, key)
	if bgErr != nil {
		return galaxycache.BackendGetInfo{}, bgErr
	}
	if d, ok := dest.(*CodecV2[C, T]); ok {
		d.Set(out)
		return bgInfo, nil
	}
	return bgInfo, setSlowV2(out, dest)
}
