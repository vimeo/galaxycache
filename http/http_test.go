/*
Copyright 2013 Google Inc.
Copyright 2019-2025 Vimeo Inc.

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

package http

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gc "github.com/vimeo/galaxycache"
	"github.com/vimeo/galaxycache/consistenthash/chtest"

	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
)

type testStatsExporter struct {
	mu   sync.Mutex
	data []*view.Data
	t    *testing.T
}

func TestHTTPHandler(t *testing.T) {

	const (
		nRoutines = 5
		nGets     = 100
	)

	for _, tbl := range []struct {
		name       string
		enablePeek bool
		setExpiry  bool
	}{
		{name: "peerFetchTest", enablePeek: false, setExpiry: false},
		{name: "peerFetchTestWithSlash/foobar", enablePeek: false, setExpiry: false},
		{name: "peerFetchWithPeek", enablePeek: true, setExpiry: false},
		{name: "peerFetchWithExpiry", enablePeek: true, setExpiry: true},
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
			universe := gc.NewUniverse(NewHTTPFetchProtocol(nil), selfName, gc.WithHashOpts(&hashOpts))
			serveMux := http.NewServeMux()
			RegisterHTTPHandler(universe, nil, serveMux)
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

			expTime := time.Time{}
			if tbl.setExpiry {
				expTime = time.Now().Add(time.Hour * 24)
			}

			gs := make([]*gc.Galaxy, 0, len(peerListeners))
			wg.Add(len(peerListeners))
			for i, listener := range peerListeners {
				go func() {
					defer wg.Done()
					makeHTTPServerUniverse(ctx, testUniverseSrvParams{t: t, hn: peerIDs[i], gCh: gCh, galaxyName: tbl.name,
						enablePeek: tbl.enablePeek, peers: peerAddresses, listener: listener,
						hashOpts:         hashOpts,
						prefixValPreseed: &preSeedReady,
						setExpiry:        expTime,
					})
				}()
				gs = append(gs, <-gCh)
			}

			for key := range testKeys(nGets, testKeyTweaks{}) {
				var value gc.StringCodec
				if getInfo, err := g.GetWithOptions(ctx, gc.GetOptions{}, key, &value); err != nil {
					t.Fatal(err)
				} else if !getInfo.Expiry.Equal(expTime) {
					t.Errorf("unexpected expiry: %s; expected %s", getInfo.Expiry, expTime)
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
				if getInfo, err := g.GetWithOptions(ctx, gc.GetOptions{}, key, &value); err != nil {
					t.Fatal(err)
				} else if !getInfo.Expiry.Equal(expTime) {
					t.Errorf("unexpected expiry: %s; expected %s", getInfo.Expiry, expTime)
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
						t.Fatal(err)
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
					if getInfo, err := og.GetWithOptions(ctx, gc.GetOptions{FetchMode: gc.FetchModeNoPeerBackend}, key, &value); err != nil {
						t.Fatal(err)
					} else if !getInfo.Expiry.Equal(expTime) {
						t.Errorf("unexpected expiry: %s; expected %s", getInfo.Expiry, expTime)
					}
					peekKeyValPrefixes[key] = "pre-seed:" + gName
				}

				preSeedReady.Store(false)

				// one more time ... this time with keys that should have been seeded on all instances
				for key := range peekKeyValPrefixes {
					var value gc.StringCodec
					if getInfo, err := g.GetWithOptions(ctx, gc.GetOptions{}, key, &value); err != nil {
						t.Fatal(err)
					} else if !getInfo.Expiry.Equal(expTime) {
						t.Errorf("unexpected expiry: %s; expected %s", getInfo.Expiry, expTime)
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
	setExpiry  time.Time

	hashOpts gc.HashOptions

	// prefix the value with the string "pre-seed" (used to pre-seed some
	// keys so we can validate that peeking works correctly)
	prefixValPreseed *atomic.Bool
}

func makeHTTPServerUniverse(ctx context.Context, p testUniverseSrvParams) {
	universe := gc.NewUniverse(NewHTTPFetchProtocol(nil), p.hn, gc.WithHashOpts(&p.hashOpts))
	serveMux := http.NewServeMux()
	wrappedHandler := &ochttp.Handler{Handler: serveMux}
	RegisterHTTPHandler(universe, nil, serveMux)
	err := universe.SetPeers(p.peers...)
	if err != nil {
		p.t.Errorf("Error setting peers: %s", err)
	}
	getter := gc.GetterFuncWithInfo(func(ctx context.Context, key string, dest gc.Codec) (gc.BackendGetInfo, error) {
		if strings.HasPrefix(key, testNotFoundKeyPrefix) {
			return gc.BackendGetInfo{}, gc.TrivialNotFoundErr{}
		}
		preSeedPfx := ""
		if p.prefixValPreseed.Load() {
			preSeedPfx = "pre-seed"
		}
		dest.UnmarshalBinary([]byte(preSeedPfx + ":" + key))
		return gc.BackendGetInfo{Expiration: p.setExpiry}, nil
	})
	gOpts := []gc.GalaxyOption{}
	if p.enablePeek {
		gOpts = append(gOpts, gc.WithPreviousPeerPeeking(gc.PeekPeerCfg{
			PeekTimeout: time.Hour,
			WarmTime:    time.Hour,
		}))
	}
	p.gCh <- universe.NewGalaxyWithBackendInfo(p.galaxyName, 1<<20, getter, gOpts...)
	newServer := http.Server{Handler: wrappedHandler}
	go func() {
		err := newServer.Serve(p.listener)
		if err != http.ErrServerClosed {
			p.t.Errorf("serve failed: %s", err)
		}
	}()

	<-ctx.Done()
	newServer.Shutdown(ctx)
}

type testKeyTweaks struct {
	pfx, suffix string
	joinCopies  int
	joinSep     string
}

func testKeys(n int, tweaks testKeyTweaks) iter.Seq[string] {
	return func(yield func(string) bool) {
		for i := range n {
			baseVal := strconv.Itoa(i)
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
