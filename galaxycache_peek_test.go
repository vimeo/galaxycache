package galaxycache

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vimeo/galaxycache/consistenthash/chtest"
	"github.com/vimeo/go-clocks/fake"
)

type testHelpStrGetter func(key string) (string, error)

// Get populates dest with the value identified by key
// The returned data must be unversioned. That is, key must
// uniquely describe the loaded data, without an implicit
// current time, and without relying on cache expiration
// mechanisms.
func (t testHelpStrGetter) Get(_ context.Context, key string, dest Codec) error {
	v, err := t(key)
	if err != nil {
		return err
	}
	return dest.UnmarshalBinary([]byte(v))
}

func TestGalaxycacheGetWithPeek(t *testing.T) {
	// define some peer names
	const (
		self  = "We are the champions!"
		peer0 = "peer0"
		peer1 = "peer1"
		peer2 = "peer2"
	)
	// define the peer addresses
	const (
		peer0Addr = "peer0.addr.internal"
		peer1Addr = "peer1.addr.internal"
		peer2Addr = "peer2.addr.internal"
	)
	chArgs := chtest.NewMapArgs(chtest.Args{Owners: []string{self, peer0, peer1, peer2}})
	peers := [...]Peer{
		{ID: peer0, URI: peer0Addr}, {ID: peer1, URI: peer1Addr}, {ID: peer2, URI: peer2Addr},
	}

	const warmPeriod = time.Hour

	type checkStep struct {
		key         string
		expVal      string
		expErr      bool
		checkErr    func(t testing.TB, idx int, getErr error)
		timeoutPeek bool // arrange for a timeout

		getOpts GetOptions
	}

	baseTime := time.Now()
	for _, tbl := range []struct {
		name         string
		includePeers []int
		includeSelf  bool
		setClock     time.Time

		// getter for locally fetched values (may call t.Error/Errorf,
		// but not t.Fatal/Fatalf, as it might be called from another
		// goroutine).
		localGetter func(t testing.TB, key string) (string, error)
		peekModes   map[string]testPeekMode  // configure peek modes per-host
		fetchModes  map[string]testFetchMode // configure fetch modes per-host

		checkSteps []checkStep

		expPeeks map[string]int
	}{{
		name:         "peeks_hit_each_peer",
		includePeers: []int{0, 1, 2},
		includeSelf:  true,
		setClock:     baseTime,
		localGetter: func(t testing.TB, key string) (string, error) {
			t.Errorf("unexpected local fetch (peek should succeed on peer)")
			return "", fmt.Errorf("unexpected call")
		},
		peekModes: map[string]testPeekMode{
			peer0Addr: testPeekModeHit,
			peer1Addr: testPeekModeHit,
			peer2Addr: testPeekModeHit,
		},
		checkSteps: []checkStep{
			{
				key:    chtest.FallthroughKey(self, peer0),
				expVal: peer0Addr + ": peek got: " + chtest.FallthroughKey(self, peer0),
			},
			{
				// second fetch, since the key should now be in the main cache
				key:    chtest.FallthroughKey(self, peer0),
				expVal: peer0Addr + ": peek got: " + chtest.FallthroughKey(self, peer0),
				getOpts: GetOptions{
					FetchMode: FetchModePeek,
				},
			},
			{
				key:    chtest.FallthroughKey(self, peer1),
				expVal: peer1Addr + ": peek got: " + chtest.FallthroughKey(self, peer1),
			},
			{
				key:    chtest.FallthroughKey(self, peer1),
				expVal: peer1Addr + ": peek got: " + chtest.FallthroughKey(self, peer1),
				getOpts: GetOptions{
					FetchMode: FetchModePeek,
				},
			},
			{
				key:    chtest.FallthroughKey(self, peer2),
				expVal: peer2Addr + ": peek got: " + chtest.FallthroughKey(self, peer2),
			},
			{
				key:    chtest.FallthroughKey(self, peer2),
				expVal: peer2Addr + ": peek got: " + chtest.FallthroughKey(self, peer2),
				getOpts: GetOptions{
					FetchMode: FetchModePeek,
				},
			},
		},
		expPeeks: map[string]int{
			peer0Addr: 1,
			peer1Addr: 1,
			peer2Addr: 1,
		},
	}, {
		name:         "peeks_timeout_each_peer",
		includePeers: []int{0, 1, 2},
		includeSelf:  true,
		setClock:     baseTime,
		localGetter: func(t testing.TB, key string) (string, error) {
			return "fizzlebat: " + key, nil
		},
		peekModes: map[string]testPeekMode{
			peer0Addr: testPeekModeStallCtx,
			peer1Addr: testPeekModeStallCtx,
			peer2Addr: testPeekModeStallCtx,
		},
		checkSteps: []checkStep{
			{
				key:         chtest.FallthroughKey(self, peer0),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer0),
				timeoutPeek: true,
			},
			{
				key:         chtest.FallthroughKey(self, peer1),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer1),
				timeoutPeek: true,
			},
			{
				key:         chtest.FallthroughKey(self, peer2),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer2),
				timeoutPeek: true,
			},
		},
		expPeeks: map[string]int{
			peer0Addr: 0,
			peer1Addr: 0,
			peer2Addr: 0,
		},
	}, {
		name:         "peeks_miss_each_peer",
		includePeers: []int{0, 1, 2},
		includeSelf:  true,
		setClock:     baseTime,
		localGetter: func(t testing.TB, key string) (string, error) {
			return "fizzlebat: " + key, nil
		},
		peekModes: map[string]testPeekMode{
			peer0Addr: testPeekModeMiss,
			peer1Addr: testPeekModeMiss,
			peer2Addr: testPeekModeMiss,
		},
		checkSteps: []checkStep{
			{
				key:         chtest.FallthroughKey(self, peer0),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer0),
				timeoutPeek: false,
			},
			{
				key:         chtest.FallthroughKey(self, peer1),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer1),
				timeoutPeek: false,
			},
			{
				key:         chtest.FallthroughKey(self, peer2),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer2),
				timeoutPeek: false,
			},
		},
		expPeeks: map[string]int{
			peer0Addr: 1,
			peer1Addr: 1,
			peer2Addr: 1,
		},
	}, {
		name:         "peeks_error_each_peer",
		includePeers: []int{0, 1, 2},
		includeSelf:  true,
		setClock:     baseTime,
		localGetter: func(t testing.TB, key string) (string, error) {
			return "fizzlebat: " + key, nil
		},
		peekModes: map[string]testPeekMode{
			peer0Addr: testPeekModeFail,
			peer1Addr: testPeekModeFail,
			peer2Addr: testPeekModeFail,
		},
		checkSteps: []checkStep{
			{
				key:         chtest.FallthroughKey(self, peer0),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer0),
				timeoutPeek: false,
			},
			{
				key:         chtest.FallthroughKey(self, peer1),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer1),
				timeoutPeek: false,
			},
			{
				key:         chtest.FallthroughKey(self, peer2),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer2),
				timeoutPeek: false,
			},
		},
		expPeeks: map[string]int{
			peer0Addr: 1,
			peer1Addr: 1,
			peer2Addr: 1,
		},
	}, {
		name:         "no_peeks_past_warm",
		includePeers: []int{0, 1, 2},
		includeSelf:  true,
		setClock:     baseTime.Add(warmPeriod),
		localGetter: func(t testing.TB, key string) (string, error) {
			return "fizzlebat: " + key, nil
		},
		peekModes: map[string]testPeekMode{
			peer0Addr: testPeekModeHit,
			peer1Addr: testPeekModeHit,
			peer2Addr: testPeekModeHit,
		},
		checkSteps: []checkStep{
			{
				key:         chtest.FallthroughKey(self, peer0),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer0),
				timeoutPeek: false,
			},
			{
				key:         chtest.FallthroughKey(self, peer1),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer1),
				timeoutPeek: false,
			},
			{
				key:         chtest.FallthroughKey(self, peer2),
				expVal:      "fizzlebat: " + chtest.FallthroughKey(self, peer2),
				timeoutPeek: false,
			},
		},
		expPeeks: map[string]int{
			peer0Addr: 0,
			peer1Addr: 0,
			peer2Addr: 0,
		},
	}, {
		name:         "not_found_remote_no_local_no_peeks",
		includePeers: []int{0, 1, 2},
		includeSelf:  true,
		setClock:     baseTime.Add(warmPeriod),
		localGetter: func(t testing.TB, key string) (string, error) {
			t.Errorf("unexpected local fetch (fetch should fail with NotFound on peer)")
			return "", fmt.Errorf("unexpected call")
		},
		peekModes: map[string]testPeekMode{
			peer0Addr: testPeekModeMiss,
			peer1Addr: testPeekModeMiss,
			peer2Addr: testPeekModeMiss,
		},
		fetchModes: map[string]testFetchMode{
			peer0Addr: testFetchModeMiss,
			peer1Addr: testFetchModeMiss,
			peer2Addr: testFetchModeMiss,
		},
		checkSteps: []checkStep{
			{
				key:         chtest.SingleOwnerKey(peer0),
				expVal:      "",
				timeoutPeek: false,
				expErr:      true,
				checkErr: func(t testing.TB, idx int, getErr error) {
					if nfErr := NotFoundErr(nil); !errors.As(getErr, &nfErr) {
						t.Errorf("error at step %d is not a not-found: %s", idx, getErr)
					}
				},
			},
			{
				key:         chtest.SingleOwnerKey(peer1),
				expVal:      "",
				timeoutPeek: false,
				expErr:      true,
				checkErr: func(t testing.TB, idx int, getErr error) {
					if nfErr := NotFoundErr(nil); !errors.As(getErr, &nfErr) {
						t.Errorf("error at step %d is not a not-found: %s", idx, getErr)
					}
				},
			},
			{
				key:         chtest.SingleOwnerKey(peer2),
				expVal:      "",
				timeoutPeek: false,
				expErr:      true,
				checkErr: func(t testing.TB, idx int, getErr error) {
					if nfErr := NotFoundErr(nil); !errors.As(getErr, &nfErr) {
						t.Errorf("error at step %d is not a not-found: %s", idx, getErr)
					}
				},
			},
		},
		expPeeks: map[string]int{
			peer0Addr: 0,
			peer1Addr: 0,
			peer2Addr: 0,
		},
	}, {
		name:         "no_fetches_force_local_remote_owned",
		includePeers: []int{0, 1, 2},
		includeSelf:  true,
		setClock:     baseTime.Add(warmPeriod),
		localGetter: func(t testing.TB, key string) (string, error) {
			return "fizzlebat: " + key, nil
		},
		peekModes: map[string]testPeekMode{
			peer0Addr: testPeekModeHit,
			peer1Addr: testPeekModeHit,
			peer2Addr: testPeekModeHit,
		},
		fetchModes: map[string]testFetchMode{
			peer0Addr: testFetchModeFailTest,
			peer1Addr: testFetchModeFailTest,
			peer2Addr: testFetchModeFailTest,
		},
		checkSteps: []checkStep{
			{
				key:         chtest.SingleOwnerKey(peer0),
				expVal:      "fizzlebat: " + chtest.SingleOwnerKey(peer0),
				timeoutPeek: false,
				getOpts: GetOptions{
					FetchMode: FetchModeNoPeerBackend,
				},
			},
			{
				key:         chtest.SingleOwnerKey(peer1),
				expVal:      "fizzlebat: " + chtest.SingleOwnerKey(peer1),
				timeoutPeek: false,
				getOpts: GetOptions{
					FetchMode: FetchModeNoPeerBackend,
				},
			},
			{
				key:         chtest.SingleOwnerKey(peer2),
				expVal:      "fizzlebat: " + chtest.SingleOwnerKey(peer2),
				timeoutPeek: false,
				getOpts: GetOptions{
					FetchMode: FetchModeNoPeerBackend,
				},
			},
		},
		expPeeks: map[string]int{
			peer0Addr: 0,
			peer1Addr: 0,
			peer2Addr: 0,
		},
	}, {
		name:         "not_found_local_peeks",
		includePeers: []int{0, 1, 2},
		includeSelf:  true,
		setClock:     baseTime.Add(warmPeriod),
		localGetter: func(t testing.TB, key string) (string, error) {
			t.Errorf("unexpected local fetch (fetch should fail with NotFound on peer)")
			return "", fmt.Errorf("unexpected call")
		},
		peekModes: map[string]testPeekMode{
			peer0Addr: testPeekModeMiss,
			peer1Addr: testPeekModeMiss,
			peer2Addr: testPeekModeMiss,
		},
		fetchModes: map[string]testFetchMode{
			peer0Addr: testFetchModeMiss,
			peer1Addr: testFetchModeMiss,
			peer2Addr: testFetchModeMiss,
		},
		checkSteps: []checkStep{
			{
				key:         chtest.SingleOwnerKey(peer0),
				expVal:      "",
				timeoutPeek: false,
				expErr:      true,
				checkErr: func(t testing.TB, idx int, getErr error) {
					if nfErr := NotFoundErr(nil); !errors.As(getErr, &nfErr) {
						t.Errorf("error at step %d is not a not-found: %s", idx, getErr)
					}
				},
				getOpts: GetOptions{
					FetchMode: FetchModePeek,
				},
			},
			{
				key:         chtest.SingleOwnerKey(peer1),
				expVal:      "",
				timeoutPeek: false,
				expErr:      true,
				checkErr: func(t testing.TB, idx int, getErr error) {
					if nfErr := NotFoundErr(nil); !errors.As(getErr, &nfErr) {
						t.Errorf("error at step %d is not a not-found: %s", idx, getErr)
					}
				},
				getOpts: GetOptions{
					FetchMode: FetchModePeek,
				},
			},
			{
				key:         chtest.SingleOwnerKey(peer2),
				expVal:      "",
				timeoutPeek: false,
				expErr:      true,
				checkErr: func(t testing.TB, idx int, getErr error) {
					if nfErr := NotFoundErr(nil); !errors.As(getErr, &nfErr) {
						t.Errorf("error at step %d is not a not-found: %s", idx, getErr)
					}
				},
				getOpts: GetOptions{
					FetchMode: FetchModePeek,
				},
			},
		},
		expPeeks: map[string]int{
			peer0Addr: 0,
			peer1Addr: 0,
			peer2Addr: 0,
		},
	}, {
		name:         "not_found_local_only",
		includePeers: []int{0, 1, 2},
		includeSelf:  true,
		setClock:     baseTime.Add(warmPeriod),
		localGetter: func(t testing.TB, key string) (string, error) {
			return "", TrivialNotFoundErr{}
		},
		peekModes: map[string]testPeekMode{
			peer0Addr: testPeekModeMiss,
			peer1Addr: testPeekModeMiss,
			peer2Addr: testPeekModeMiss,
		},
		fetchModes: map[string]testFetchMode{
			peer0Addr: testFetchModeFailTest,
			peer1Addr: testFetchModeFailTest,
			peer2Addr: testFetchModeFailTest,
		},
		checkSteps: []checkStep{
			{
				key:         chtest.SingleOwnerKey(peer0),
				expVal:      "",
				timeoutPeek: false,
				expErr:      true,
				checkErr: func(t testing.TB, idx int, getErr error) {
					if nfErr := NotFoundErr(nil); !errors.As(getErr, &nfErr) {
						t.Errorf("error at step %d is not a not-found: %s", idx, getErr)
					}
				},
				getOpts: GetOptions{
					FetchMode: FetchModeNoPeerBackend,
				},
			},
			{
				key:         chtest.SingleOwnerKey(peer1),
				expVal:      "",
				timeoutPeek: false,
				expErr:      true,
				checkErr: func(t testing.TB, idx int, getErr error) {
					if nfErr := NotFoundErr(nil); !errors.As(getErr, &nfErr) {
						t.Errorf("error at step %d is not a not-found: %s", idx, getErr)
					}
				},
				getOpts: GetOptions{
					FetchMode: FetchModeNoPeerBackend,
				},
			},
			{
				key:         chtest.SingleOwnerKey(peer2),
				expVal:      "",
				timeoutPeek: false,
				expErr:      true,
				checkErr: func(t testing.TB, idx int, getErr error) {
					if nfErr := NotFoundErr(nil); !errors.As(getErr, &nfErr) {
						t.Errorf("error at step %d is not a not-found: %s", idx, getErr)
					}
				},
				getOpts: GetOptions{
					FetchMode: FetchModeNoPeerBackend,
				},
			},
		},
		expPeeks: map[string]int{
			peer0Addr: 0,
			peer1Addr: 0,
			peer2Addr: 0,
		},
	},
	} {
		t.Run(tbl.name, func(t *testing.T) {
			fc := fake.NewClock(baseTime)

			fp := TestProtocol{
				TestFetchers: map[string]*TestFetcher{},
				dialFails:    map[string]struct{}{},
				peekModes:    tbl.peekModes,
				fetchModes:   tbl.fetchModes,
				hostInVal:    true,
				clk:          fc,
			}
			u := NewUniverse(&fp, self, WithHashOpts(
				&HashOptions{Replicas: chArgs.NSegsPerKey, HashFn: chArgs.HashFunc}),
				WithUniversalClock(fc))
			u.SetIncludeSelf(tbl.includeSelf)
			for _, pIdx := range tbl.includePeers {
				t.Logf("adding peer %+v", peers[pIdx])
				u.AddPeer(peers[pIdx])
			}
			defer u.Shutdown()

			getter := testHelpStrGetter(func(key string) (string, error) {
				return tbl.localGetter(t, key)
			})

			peekTimeout := time.Millisecond * 3
			g := u.NewGalaxy("easy come; easy go", 256, getter, WithPreviousPeerPeeking(PeekPeerCfg{
				WarmTime:    warmPeriod,
				PeekTimeout: peekTimeout},
			))

			fc.SetClock(tbl.setClock)
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			// now, we have a fully configured universe (except for the galaxy)
			for i, step := range tbl.checkSteps {
				c := StringCodec("")
				var getErr error
				if !step.timeoutPeek {
					_, getErr = g.GetWithOptions(ctx, step.getOpts, step.key, &c)
				} else {
					doneCh := make(chan struct{})
					go func() {
						defer close(doneCh)
						_, getErr = g.GetWithOptions(ctx, step.getOpts, step.key, &c)
					}()
					fc.AwaitSleepers(1)
					fc.Advance(peekTimeout)

					<-doneCh
				}
				if getErr != nil {
					if step.expErr {
						if step.checkErr != nil {
							step.checkErr(t, i, getErr)
						}
						// expected error; keep going
						continue
					}
					t.Errorf("unexpected error fetching step %d; key %q: %s", i, step.key, getErr)
					continue
				}
				if string(c) != step.expVal {
					t.Errorf("unexpected value for key %q in step %d\nwant: %q\n got %q", step.key, i, step.expVal, c)
				}
			}
			for peerAddr, expPeeks := range tbl.expPeeks {
				if fetcher, ok := fp.TestFetchers[peerAddr]; ok {
					if fetcher.peeks != expPeeks {
						t.Errorf("unexpected peek-count on addr %q: got %d, want %d", peerAddr, fetcher.peeks, expPeeks)
					}
				} else if expPeeks != 0 {
					t.Errorf("unexpected peek-count on addr %q: got %d, want %d", peerAddr, 0, expPeeks)
				}
			}
		})
	}
}
