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

// Package peercfg defines the [Config] struct that is shared between both the
// test driver and the test peers (which may be running different versions).
// This type will be passed as a JSON blob to stdin for the peer subprocesses
// to configure Galaxycache (some portions may be optional depending on the
// Galaxycache version).
package peercfg

import "time"

// Peer provides the relevant information about this peer
type Peer struct {
	Name    string
	Address string
}

type PeekConfig struct {
	Timeout  time.Duration
	WarmTime time.Duration
}

type GalaxyHandlerMode uint8

const (
	HandlerSuccess GalaxyHandlerMode = iota
	HandlerFail
	HandlerNotFound     // shouldn't be set for peers that are too old to support it
	HandlerGRPCNotFound // can always be set
)

// ListenMode indicates which servers to setup
type ListenMode uint8

const (
	// use FD3 for the listener on the gRPC server
	ListenGRPC ListenMode = iota
	// use FD3 for the listener on the HTTP server
	ListenHTTP
	// use FD3 for the listener on the gRPC server and FD 4 for the
	// listener on the HTTP server
	ListenGRPCHTTP
)

// PeerFetcherMode indicates which PeerFetcher implementation to register with
// the universe
type PeerFetcherMode uint8

const (
	FetchGRPC PeerFetcherMode = iota
	FetchHTTP
)

// Galaxy provides information about initializing this galaxy
type Galaxy struct {
	Name           string
	EchoKey        bool
	PrefixSelfName bool
	Bytes          uint64
	HydrationMode  GalaxyHandlerMode

	Peek *PeekConfig
}

// Config provides the key attributes of the peer
type Config struct {
	SelfName string
	Peers    []Peer
	IncSelf  bool

	// Preregister these keys to the peer in the value
	PreRegisterKeys map[string]string

	FetchProto PeerFetcherMode
	ListenMode ListenMode

	Galaxies []Galaxy
}
