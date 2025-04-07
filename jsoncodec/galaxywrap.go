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

	"github.com/vimeo/galaxycache"
)

// GalaxyGet is a simple wrapper around a Galaxy.Get method-call that takes
// care of constructing the [Codec], etc. (making the interface more idiomatic for Go)
func GalaxyGet[T any](ctx context.Context, g *galaxycache.Galaxy, key string) (m *T, getErr error) {
	pc := Codec[T]{}
	getErr = g.Get(ctx, key, &pc)
	if getErr != nil {
		return // use named return values to bring the inlining cost down
	}
	return &pc.msg, nil
}
