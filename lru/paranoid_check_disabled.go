//go:build !paranoidgcll

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

package lru

// Default-disable paranoid checks so they get compiled out.
// This file is build-tagged so these checks can easily be enabled or disabled.
// The `paranoidgcll` build tag breaks down as "paranoid Galaxycache Linked List".
const paranoidLL = false
