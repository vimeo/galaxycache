/*
 Copyright 2019-2025 Vimeo Inc.
 Adapted from https://github.com/golang/groupcache/blob/master/http_test.go

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

package grpc

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gc "github.com/vimeo/galaxycache"
	"github.com/vimeo/galaxycache/consistenthash/chtest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCPeerServer(t *testing.T) {

	const (
		nRoutines = 5
		nGets     = 100
	)

	for _, tbl := range []struct {
		name       string
		enablePeek bool
	}{
		{name: "peerFetchTest", enablePeek: false},
		{name: "peerFetchTestWithSlash/foobar", enablePeek: false},
		{name: "peerFetchWithPeek", enablePeek: true},
	} {
		t.Run(tbl.name, func(t *testing.T) {

			var peerAddresses []gc.Peer
			var peerIDs []string
			var peerListeners []net.Listener

			for i := range nRoutines {
				hn := fmt.Sprintf("peer-%03d", i)
				newListener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				peerAddresses = append(peerAddresses, gc.Peer{
					URI: newListener.Addr().String(),
					ID:  hn,
				})
				peerIDs = append(peerIDs, hn)
				peerListeners = append(peerListeners, newListener)
			}

			// pre-register all the keys that we'll be using
			regKeys := map[string][]string{}
			for key := range testKeys(nGets, testKeyTweaks{}) {
				ownerIdx := (len(regKeys[key]) * 3) % len(peerIDs)
				regKeys[key] = []string{peerIDs[ownerIdx]}
			}
			for key := range testKeys(nGets, testKeyTweaks{joinCopies: 2, joinSep: "/"}) {
				ownerIdx := (len(regKeys[key]) * 3) % len(peerIDs)
				regKeys[key] = []string{peerIDs[ownerIdx]}
			}
			for key := range testKeys(nGets, testKeyTweaks{pfx: testNotFoundKeyPrefix}) {
				ownerIdx := (len(regKeys[key]) * 3) % len(peerIDs)
				regKeys[key] = []string{peerIDs[ownerIdx]}
			}

			const selfName = "shouldBeIgnored"
			ma := chtest.NewMapArgs(chtest.Args{
				Owners:       append(peerIDs, selfName),
				RegisterKeys: regKeys,
			})
			hashOpts := gc.HashOptions{
				Replicas: ma.NSegsPerKey,
				HashFn:   ma.HashFunc,
			}
			universe := gc.NewUniverse(NewGRPCFetchProtocol(grpc.WithTransportCredentials(insecure.NewCredentials())), selfName, gc.WithHashOpts(&hashOpts))
			defer func() {
				shutdownErr := universe.Shutdown()
				if shutdownErr != nil {
					t.Errorf("Error on shutdown: %s", shutdownErr)
				}
			}()
			err := universe.SetPeers(peerAddresses...)
			if err != nil {
				t.Errorf("Error setting peers: %s", err)
			}
			universe.SetIncludeSelf(false) // remove self from hashring

			getter := gc.GetterFunc(func(ctx context.Context, key string, dest gc.Codec) error {
				return fmt.Errorf("oh no! Local get occurred")
			})
			g := universe.NewGalaxy(tbl.name, 1<<20, getter)

			wg := sync.WaitGroup{}
			defer wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			gCh := make(chan *gc.Galaxy, len(peerListeners))

			preSeedReady := atomic.Bool{}

			gs := make([]*gc.Galaxy, 0, len(peerListeners))
			wg.Add(len(peerListeners))
			for i, listener := range peerListeners {
				go func() {
					defer wg.Done()
					runTestPeerGRPCServer(ctx, testUniverseSrvParams{t: t, hn: peerIDs[i], gCh: gCh, galaxyName: tbl.name,
						enablePeek: tbl.enablePeek, peers: peerAddresses, listener: listener,
						hashOpts:         hashOpts,
						prefixValPreseed: &preSeedReady}, &wg)
				}()
				gs = append(gs, <-gCh)
			}

			preSeedReady.Store(false)

			for key := range testKeys(nGets, testKeyTweaks{}) {
				var value gc.StringCodec
				if err := g.Get(ctx, key, &value); err != nil {
					t.Fatalf("fetch of key %q failed: %s", key, err)
				}
				if suffix := ":" + key; !strings.HasSuffix(string(value), suffix) {
					t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
				}
				t.Logf("Get key=%q, value=%q (peer:key)", key, value)
			}
			// Try it again, this time with a slash in the middle to ensure we're
			// handling those characters properly
			for key := range testKeys(nGets, testKeyTweaks{joinCopies: 2, joinSep: "/"}) {
				var value gc.StringCodec
				if err := g.Get(ctx, key, &value); err != nil {
					t.Fatal(err)
				}
				if suffix := ":" + key; !strings.HasSuffix(string(value), suffix) {
					t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
				}
				t.Logf("Get key=%q, value=%q (peer:key)", key, value)
			}
			// ... and again
			// this time with keys that have a prefix that should generate 404s
			for key := range testKeys(nGets, testKeyTweaks{pfx: testNotFoundKeyPrefix}) {
				var value gc.StringCodec
				if err := g.Get(ctx, key, &value); err != nil {
					if nfErr := (gc.NotFoundErr)(nil); !errors.As(err, &nfErr) {
						t.Fatalf("fetch of key %q failed: %s", key, err)
					}
				} else {
					t.Errorf("get of key %q should have failed with NotFound; got value: %q", testNotFoundKeyPrefix+key, value)
				}
			}
			if tbl.enablePeek {
				preSeedReady.Store(true)
				peekKeyValPrefixes := map[string]string{}
				for i, og := range gs {
					gName := peerIDs[i]
					priKeyOwner := peerIDs[(i+1)%len(peerIDs)]
					key := chtest.FallthroughKey(priKeyOwner, gName)
					var value gc.StringCodec
					if _, err := og.GetWithOptions(ctx, gc.GetOptions{FetchMode: gc.FetchModeNoPeerBackend}, key, &value); err != nil {
						t.Fatal(err)
					}
					peekKeyValPrefixes[key] = "pre-seed:" + gName
				}
				// one more time ... this time with keys that should have been seeded on all instances
				for key := range peekKeyValPrefixes {
					var value gc.StringCodec
					if err := g.Get(ctx, key, &value); err != nil {
						t.Fatal(err)
					}
					if !strings.HasPrefix(string(value), "pre-seed:") {
						t.Errorf("Get(%q) = %q, want value starting in %q", key, value, "pre-seed")
					}
					if suffix := ":" + key; !strings.HasSuffix(string(value), suffix) {
						t.Errorf("Get(%q) = %q, want value ending in %q", key, value, suffix)
					}
					t.Logf("Get key=%q, value=%q (peer:key)", key, value)
				}
			}
		})
	}
}

const testNotFoundKeyPrefix = "not found key/"

type testUniverseSrvParams struct {
	t          testing.TB
	hn         string
	gCh        chan<- *gc.Galaxy
	galaxyName string
	enablePeek bool
	peers      []gc.Peer
	listener   net.Listener

	hashOpts gc.HashOptions

	// prefix the value with the string "pre-seed" (used to pre-seed some
	// keys so we can validate that peeking works correctly)
	prefixValPreseed *atomic.Bool
}

func runTestPeerGRPCServer(ctx context.Context, p testUniverseSrvParams, wg *sync.WaitGroup) {
	universe := gc.NewUniverse(NewGRPCFetchProtocol(grpc.WithTransportCredentials(insecure.NewCredentials())), p.hn, gc.WithHashOpts(&p.hashOpts))
	grpcServer := grpc.NewServer()
	RegisterGRPCServer(universe, grpcServer)
	err := universe.SetPeers(p.peers...)
	if err != nil {
		p.t.Errorf("Error setting peers: %s", err)
	}
	getter := gc.GetterFunc(func(ctx context.Context, key string, dest gc.Codec) error {
		if strings.HasPrefix(key, testNotFoundKeyPrefix) {
			return gc.TrivialNotFoundErr{}
		}
		preSeedPfx := ""
		if p.prefixValPreseed.Load() {
			preSeedPfx = "pre-seed"
		}
		dest.UnmarshalBinary([]byte(preSeedPfx + ":" + key))
		return nil
	})
	// Make sure we make peek calls
	p.gCh <- universe.NewGalaxy(p.galaxyName, 1<<20, getter, gc.WithPreviousPeerPeeking(gc.PeekPeerCfg{
		PeekTimeout: time.Hour,
		WarmTime:    time.Hour,
	}))
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := grpcServer.Serve(p.listener)
		if err != nil {
			p.t.Errorf("Serve failed: %s", err)
		}
	}()

	<-ctx.Done()
	grpcServer.GracefulStop()
}

type testKeyTweaks struct {
	pfx, suffix string
	joinCopies  int
	joinSep     string
}

func testKeys(n int, tweaks testKeyTweaks) iter.Seq[string] {
	return func(yield func(string) bool) {
		for i := range n {
			baseVal := strconv.Itoa(i) + "\270\201\000\001"
			if tweaks.joinCopies == 0 {
				if !yield(tweaks.pfx + baseVal + tweaks.suffix) {
					return
				}
			} else {
				if !yield(tweaks.pfx + strings.Join(
					slices.Repeat([]string{baseVal},
						tweaks.joinCopies), tweaks.joinSep) + tweaks.suffix) {
					return
				}
			}
		}
	}
}
