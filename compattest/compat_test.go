package compattest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/vimeo/galaxycache"
	"github.com/vimeo/galaxycache/compattest/peercfg"
	"github.com/vimeo/galaxycache/consistenthash/chtest"
	gcgrpc "github.com/vimeo/galaxycache/grpc"
	gchttp "github.com/vimeo/galaxycache/http"
)

func mustMarshalJSON(v any) []byte {
	out, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return out
}

type prefixedTestLogWriter struct {
	t      testing.TB
	prefix string
}

func (p *prefixedTestLogWriter) Write(b []byte) (n int, err error) {
	p.t.Log(p.prefix, string(b))
	return len(b), nil
}

func defaultFetchProtocol(fpOpt peercfg.PeerFetcherMode) galaxycache.FetchProtocol {
	switch fpOpt {
	case peercfg.FetchGRPC:
		return gcgrpc.NewGRPCFetchProtocol(grpc.WithTransportCredentials(insecure.NewCredentials()))
	case peercfg.FetchHTTP:
		return gchttp.NewHTTPFetchProtocol(nil)
	default:
		panic("unknown peer fetcher mode: " + strconv.Itoa(int(fpOpt)))
	}
}

func TestFetchModeCompat(t *testing.T) {
	type peerOpts struct {
		fetchProto peercfg.PeerFetcherMode
		listenMode peercfg.ListenMode

		includeSelf bool

		galaxySize     uint64
		hydMode        peercfg.GalaxyHandlerMode
		prefixSelfName bool
		echoKey        bool

		enablePeek bool
	}

	type fetchKeyExp struct {
		headHostIdx int // index of the "peer" to use for this fetch.
		// if true, set `u.SetIncludeSelf(false)` while handling this
		// request. (this allows for priming the cache on peers, etc.)
		skipSelf    bool
		getOpts     galaxycache.GetOptions
		key         string
		expVal      string // ignored if one of the below is set
		expNotFound bool
		expErr      bool
	}

	v12WorkerPath := filepath.Join(t.TempDir(), "peerv1.2")
	{
		peerBuildCmd := exec.Command("go", "build", "-o", v12WorkerPath, "-tags=testing_binary_chtest_can_panic_at_any_time", ".")
		peerBuildCmd.Dir = "./peerv1.2/"
		out, buildErr := peerBuildCmd.CombinedOutput()
		t.Logf("build output: %s", string(out))
		if buildErr != nil {
			t.Fatalf("failed to build v1.2 peer")
		}
	}

	for _, tbl := range []struct {
		name string
		// Names will be "<version>-<index>" (where "<version>" is either "v1.2" or "head")
		v12Peers      []peerOpts
		headPeers     []peerOpts
		headFetchKeys []fetchKeyExp
		galaxyName    string // for now, only deal with one galaxy
		preRegKeys    map[string]string
	}{
		{
			name: "one_each_vers_grpc_fetch_not_found_no_peek",
			v12Peers: []peerOpts{
				{
					fetchProto:  peercfg.FetchGRPC,
					listenMode:  peercfg.ListenGRPC,
					includeSelf: true,
					galaxySize:  1024,
					hydMode:     peercfg.HandlerGRPCNotFound,
					// ignored
					prefixSelfName: false,
					echoKey:        false,
					enablePeek:     false,
				},
			},
			headPeers: []peerOpts{
				{
					fetchProto:  peercfg.FetchGRPC,
					listenMode:  peercfg.ListenGRPC,
					includeSelf: true,
					galaxySize:  1024,
					hydMode:     peercfg.HandlerNotFound,
					// ignored
					prefixSelfName: false,
					echoKey:        false,
					enablePeek:     false,
				},
			},
			headFetchKeys: []fetchKeyExp{
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "v1.2-1"),
					expVal:      "",
					expNotFound: true,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("v1.2-0", "head-0"),
					expVal:      "",
					expNotFound: true,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         chtest.FallthroughKey("v1.2-0", "head-0"),
					expVal:      "",
					expNotFound: true,
					expErr:      false,
				},
			},
			galaxyName: "fizzlebat",
		},
		{
			name: "one_each_vers_grpc_fetch_echo_keys_no_peek",
			v12Peers: []peerOpts{
				{
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
			},
			headPeers: []peerOpts{
				{
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
			},
			headFetchKeys: []fetchKeyExp{
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.SingleOwnerKey("head-0"),
					expVal:      "head-0: " + chtest.SingleOwnerKey("head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("v1.2-0", "head-0"),
					expVal:      "v1.2-0: " + chtest.FallthroughKey("v1.2-0", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         chtest.FallthroughKey("v1.2-0", "head-0"),
					expVal:      "v1.2-0: " + chtest.FallthroughKey("v1.2-0", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
			},
			galaxyName: "fizzlebat",
		},
		{
			name:     "no_peek_3_head_only_grpc",
			v12Peers: []peerOpts{},
			headPeers: []peerOpts{
				{
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
				{
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
				{
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
			},
			headFetchKeys: []fetchKeyExp{
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "", // ignored
					expNotFound: true,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    true,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}", // already cached
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.SingleOwnerKey("head-0"),
					expVal:      "head-0: " + chtest.SingleOwnerKey("head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-2", "head-0"),
					expVal:      "head-2: " + chtest.FallthroughKey("head-2", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    true, // fetch from head-0
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-1", "head-0"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-1", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    true, // now cached, so irrelevant
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         chtest.FallthroughKey("head-1", "head-0"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-1", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
			},
			galaxyName: "fizzlebat",
		},
		{
			name: "3_head_2_v1.2_peeking_grpc",
			v12Peers: []peerOpts{
				{
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				}, {
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
			},
			headPeers: []peerOpts{
				{
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     true,
				}, {
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     true,
				}, {
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPC,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     true,
				},
			},
			headFetchKeys: []fetchKeyExp{
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "", // ignored
					expNotFound: true,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					// this should send a peek to head-1, but that cache is empty, so it'll get filled locally
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    true,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}", // already cached
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "v1.2-0"), // the peek request should error and keep going here
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "v1.2-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("v1.2-0", "head-0"), // there won't be a peek request
					expVal:      "v1.2-0: " + chtest.FallthroughKey("v1.2-0", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("v1.2-1", "head-0"), // there won't be a peek request
					expVal:      "v1.2-1: " + chtest.FallthroughKey("v1.2-1", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 2,
					skipSelf:    false,
					// seed the peek peer
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeNoPeerBackend},
					key:         chtest.FallthroughKey("head-1", "head-2"),
					expVal:      "head-2: " + chtest.FallthroughKey("head-1", "head-2") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					// fetch normally, so head-1 makes a peek request to head-2
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-1", "head-2"),
					expVal:      "head-2: " + chtest.FallthroughKey("head-1", "head-2") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
			},
			galaxyName: "fizzlebat",
		},
		{
			name:       "one_each_vers_http_fetch_not_found_no_peek",
			preRegKeys: map[string]string{"abc": "v1.2-0", "def": "head-0"},
			v12Peers: []peerOpts{
				{
					fetchProto:  peercfg.FetchHTTP,
					listenMode:  peercfg.ListenHTTP,
					includeSelf: true,
					galaxySize:  1024,
					hydMode:     peercfg.HandlerGRPCNotFound,
					// ignored
					prefixSelfName: false,
					echoKey:        false,
					enablePeek:     false,
				},
			},
			headPeers: []peerOpts{
				{
					fetchProto:  peercfg.FetchHTTP,
					listenMode:  peercfg.ListenHTTP,
					includeSelf: true,
					galaxySize:  1024,
					hydMode:     peercfg.HandlerNotFound,
					// ignored
					prefixSelfName: false,
					echoKey:        false,
					enablePeek:     false,
				},
			},
			headFetchKeys: []fetchKeyExp{
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         "def",
					expVal:      "",
					expNotFound: true,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         "abc",
					expVal:      "",
					expNotFound: true,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         "abc",
					expVal:      "",
					expNotFound: true,
					expErr:      false,
				},
			},
			galaxyName: "fizzlebat",
		},
		{
			name:       "one_each_vers_http_fetch_echo_keys_no_peek",
			preRegKeys: map[string]string{"abc": "v1.2-0", "def": "head-0"},
			v12Peers: []peerOpts{
				{
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
			},
			headPeers: []peerOpts{
				{
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
			},
			headFetchKeys: []fetchKeyExp{
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         "def",
					expVal:      "head-0: " + "def" + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         "abc",
					expVal:      "v1.2-0: " + "abc" + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         "abc",
					expVal:      "v1.2-0: " + "abc" + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
			},
			galaxyName: "fizzlebat",
		},
		{
			name:     "no_peek_3_head_only_http",
			v12Peers: []peerOpts{},
			headPeers: []peerOpts{
				{
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
				{
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
				{
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
			},
			headFetchKeys: []fetchKeyExp{
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "", // ignored
					expNotFound: true,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    true,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}", // already cached
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.SingleOwnerKey("head-0"),
					expVal:      "head-0: " + chtest.SingleOwnerKey("head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-2", "head-0"),
					expVal:      "head-2: " + chtest.FallthroughKey("head-2", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    true, // fetch from head-0
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-1", "head-0"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-1", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    true, // now cached, so irrelevant
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         chtest.FallthroughKey("head-1", "head-0"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-1", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
			},
			galaxyName: "fizzlebat",
		},
		{
			name: "3_head_2_v1.2_peeking_http",
			v12Peers: []peerOpts{
				{
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				}, {
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
			},
			headPeers: []peerOpts{
				{
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     true,
				}, {
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     true,
				}, {
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     true,
				},
			},
			headFetchKeys: []fetchKeyExp{
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "", // ignored
					expNotFound: true,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					// this should send a peek to head-1, but that cache is empty, so it'll get filled locally
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    true,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}", // already cached
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "v1.2-0"), // the peek request should error and keep going here
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "v1.2-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("v1.2-0", "head-0"), // there won't be a peek request
					expVal:      "v1.2-0: " + chtest.FallthroughKey("v1.2-0", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("v1.2-1", "head-0"), // there won't be a peek request
					expVal:      "v1.2-1: " + chtest.FallthroughKey("v1.2-1", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 2,
					skipSelf:    false,
					// seed the peek peer
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeNoPeerBackend},
					key:         chtest.FallthroughKey("head-1", "head-2"),
					expVal:      "head-2: " + chtest.FallthroughKey("head-1", "head-2") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					// fetch normally, so head-1 makes a peek request to head-2
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-1", "head-2"),
					expVal:      "head-2: " + chtest.FallthroughKey("head-1", "head-2") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
			},
			galaxyName: "fizzlebat",
		},
		{
			name: "3_head_2_v1.2_peeking_http_mixed_fetching",
			v12Peers: []peerOpts{
				{
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenGRPCHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				}, {
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenGRPCHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     false,
				},
			},
			headPeers: []peerOpts{
				{
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenGRPCHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     true,
				}, {
					fetchProto:     peercfg.FetchGRPC,
					listenMode:     peercfg.ListenGRPCHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     true,
				}, {
					fetchProto:     peercfg.FetchHTTP,
					listenMode:     peercfg.ListenGRPCHTTP,
					includeSelf:    true,
					galaxySize:     1024,
					hydMode:        peercfg.HandlerSuccess,
					prefixSelfName: true,
					echoKey:        true,
					enablePeek:     true,
				},
			},
			headFetchKeys: []fetchKeyExp{
				{
					headHostIdx: 0,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModePeek},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "", // ignored
					expNotFound: true,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					// this should send a peek to head-1, but that cache is empty, so it'll get filled locally
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    true,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "head-1"),
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "head-1") + ": {some value}", // already cached
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-0", "v1.2-0"), // the peek request should error and keep going here
					expVal:      "head-0: " + chtest.FallthroughKey("head-0", "v1.2-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("v1.2-0", "head-0"), // there won't be a peek request
					expVal:      "v1.2-0: " + chtest.FallthroughKey("v1.2-0", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 1,
					skipSelf:    false,
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("v1.2-1", "head-0"), // there won't be a peek request
					expVal:      "v1.2-1: " + chtest.FallthroughKey("v1.2-1", "head-0") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 2,
					skipSelf:    false,
					// seed the peek peer
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeNoPeerBackend},
					key:         chtest.FallthroughKey("head-1", "head-2"),
					expVal:      "head-2: " + chtest.FallthroughKey("head-1", "head-2") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
				{
					headHostIdx: 0,
					skipSelf:    false,
					// fetch normally, so head-1 makes a peek request to head-2
					getOpts:     galaxycache.GetOptions{FetchMode: galaxycache.FetchModeRegular},
					key:         chtest.FallthroughKey("head-1", "head-2"),
					expVal:      "head-2: " + chtest.FallthroughKey("head-1", "head-2") + ": {some value}",
					expNotFound: false,
					expErr:      false,
				},
			},
			galaxyName: "fizzlebat",
		},
	} {
		t.Run(tbl.name, func(t *testing.T) {
			t.Parallel()
			type peerInfo struct {
				name     string
				primaryL *net.TCPListener
				secL     *net.TCPListener
				opts     peerOpts
			}

			// Setup ephemeral TCP listeners
			v12Peers := map[string]peerInfo{}
			hashPeers := []string{}
			grpcCfgPeers := []peercfg.Peer{}
			httpCfgPeers := []peercfg.Peer{}

			openListeners := func(name string, p peerOpts) peerInfo {
				pInfo := peerInfo{
					name: name,
					opts: p,
				}
				primL, primLErr := net.Listen("tcp", "localhost:0")
				if primLErr != nil {
					t.Fatalf("failed to create primary listener: %s", primLErr)
				}
				pInfo.primaryL = primL.(*net.TCPListener)
				hashPeers = append(hashPeers, name)
				switch p.listenMode {
				case peercfg.ListenHTTP:
					httpCfgPeers = append(httpCfgPeers, peercfg.Peer{
						Name:    name,
						Address: primL.Addr().String(),
					})
				case peercfg.ListenGRPCHTTP:
					secL, secLErr := net.Listen("tcp", "localhost:0")
					if secLErr != nil {
						t.Fatalf("failed to create secondary listener: %s", primLErr)
					}
					pInfo.secL = secL.(*net.TCPListener)
					httpCfgPeers = append(httpCfgPeers, peercfg.Peer{
						Name:    name,
						Address: secL.Addr().String(),
					})
					fallthrough
				case peercfg.ListenGRPC:
					grpcCfgPeers = append(grpcCfgPeers, peercfg.Peer{
						Name:    name,
						Address: primL.Addr().String(),
					})
				}
				return pInfo
			}

			for i, p := range tbl.v12Peers {
				name := "v1.2-" + strconv.Itoa(i)
				v12Peers[name] = openListeners(name, p)
			}

			headPeers := map[string]peerInfo{}
			for i, p := range tbl.headPeers {
				name := "head-" + strconv.Itoa(i)
				headPeers[name] = openListeners(name, p)
			}

			// Now that we have all the ports; time to assemble the configs for the peers and run the subprocesses.
			peerCtx, cancel := context.WithCancel(context.Background())
			defer cancel()

			subProcessPeers := []*exec.Cmd{}

			t.Log("current gRPC cfg peers:", grpcCfgPeers)
			t.Log("current HTTP cfg peers:", httpCfgPeers)

			genPeerCfg := func(pName string, pInfo peerInfo) peercfg.Config {
				peek := (*peercfg.PeekConfig)(nil)
				if pInfo.opts.enablePeek {
					peek = &peercfg.PeekConfig{
						Timeout:  time.Hour,
						WarmTime: time.Hour,
					}
				}
				peers := grpcCfgPeers
				if pInfo.opts.fetchProto == peercfg.FetchHTTP {
					peers = httpCfgPeers
				}
				return peercfg.Config{
					SelfName:        pName,
					Peers:           peers,
					IncSelf:         pInfo.opts.includeSelf,
					FetchProto:      pInfo.opts.fetchProto,
					ListenMode:      pInfo.opts.listenMode,
					PreRegisterKeys: tbl.preRegKeys,
					Galaxies: []peercfg.Galaxy{{
						Name:           tbl.galaxyName,
						EchoKey:        pInfo.opts.echoKey,
						PrefixSelfName: pInfo.opts.prefixSelfName,
						Bytes:          pInfo.opts.galaxySize,
						HydrationMode:  pInfo.opts.hydMode,
						// peek isn't implemented for v1.2 peers (but it'll be ignored there anyway)
						Peek: peek,
					}},
				}
			}

			for pName, pInfo := range v12Peers {
				cmd := exec.CommandContext(peerCtx, v12WorkerPath)

				cmd.Stdout = &prefixedTestLogWriter{t: t, prefix: pName + " (stdout): "}
				cmd.Stderr = &prefixedTestLogWriter{t: t, prefix: pName + " (stderr): "}

				primaryFD, primFileErr := pInfo.primaryL.File()
				if primFileErr != nil {
					t.Fatalf("failed to extract os.File for primary listener of peer %s: %s", pName, primFileErr)
				}
				cmd.ExtraFiles = []*os.File{primaryFD}
				if pInfo.secL != nil {
					secFD, secFileErr := pInfo.secL.File()
					if secFileErr != nil {
						t.Fatalf("failed to extract os.File for secondary listener of peer %s: %s", pName, secFileErr)
					}
					cmd.ExtraFiles = append(cmd.ExtraFiles, secFD)
				}
				pInfo.primaryL.Close()
				if pInfo.secL != nil {
					pInfo.secL.Close()
				}

				cfg := genPeerCfg(pName, pInfo)

				cmd.Stdin = bytes.NewReader(mustMarshalJSON(cfg))

				// Configured! Now Run it!
				if stErr := cmd.Start(); stErr != nil {
					t.Fatalf("failed to start v1.2 subprocess %s: %s", pName, stErr)
				}

				for _, pList := range cmd.ExtraFiles {
					pList.Close()
				}

				t.Logf("started peer %q", pName)

				subProcessPeers = append(subProcessPeers, cmd)
			}

			inProcPeerWG := sync.WaitGroup{}
			defer inProcPeerWG.Wait()
			inProcCtx, inProcCancel := context.WithCancel(context.Background())
			defer inProcCancel()

			type peerUniverseGalaxy struct {
				u  *galaxycache.Universe
				gs []*galaxycache.Galaxy
			}
			headGalaxies := make(map[string]peerUniverseGalaxy, len(headPeers))
			// spin up the peers running in-process (since they're using the same version of galaxycache)
			for pName, pInfo := range headPeers {
				pCfg := genPeerCfg(pName, pInfo)
				u, gs := genUniverse(pCfg)
				headGalaxies[pName] = peerUniverseGalaxy{
					u:  u,
					gs: gs,
				}
				inProcPeerWG.Add(1)
				go func() {
					defer inProcPeerWG.Done()
					if runErr := runPeer(inProcCtx, t, u, pInfo.primaryL, pInfo.secL, pCfg); runErr != nil {
						t.Errorf("peer %q failed: %s", pName, runErr)
					}
				}()
				t.Logf("started peer %q", pName)
			}

			///////////////////////////////////////////////////////////
			// Setup complete; Now to make a few Get calls!
			///////////////////////////////////////////////////////////
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			for i, fk := range tbl.headFetchKeys {
				ugs := headGalaxies["head-"+strconv.Itoa(fk.headHostIdx)]
				origIncSelf := ugs.u.IncludeSelf()
				ugs.u.SetIncludeSelf(!fk.skipSelf)

				g := ugs.gs[0]
				sc := galaxycache.StringCodec("")
				_, getErr := g.GetWithOptions(ctx, fk.getOpts, fk.key, &sc)
				ugs.u.SetIncludeSelf(origIncSelf) // restore the IncludeSelf value

				nfErr := (galaxycache.NotFoundErr)(nil)
				isNotFound := errors.As(getErr, &nfErr)
				if isNotFound {
					if !fk.expNotFound {
						t.Errorf("unexpected Not Found-unwrappable error for key %s (idx %d); got: %s",
							fk.key, i, getErr)
					}
					continue

				}
				if getErr != nil {
					if !fk.expErr {
						t.Errorf("expected Not Found-unwrappable error for key %s (idx %d); got: %s",
							fk.key, i, getErr)
					}
					continue
				}
				if string(sc) != fk.expVal {
					t.Errorf("unexpected value for key %q (idx %d) on %s;\n got: %q\nwant: %q",
						fk.key, i, ugs.u.SelfID(), sc, fk.expVal)
				}
			}

			///////////////////////////////////////////////////////////
			// We're done here, shutdown stuff goes below this point //
			///////////////////////////////////////////////////////////

			for i, spp := range subProcessPeers {
				sigErr := spp.Process.Signal(syscall.SIGTERM)
				if sigErr != nil {
					t.Errorf("failed to signal peer %d: %s", i, sigErr)
				}
				if wErr := spp.Wait(); wErr != nil {
					t.Errorf("subprocess failed: %v", spp.ProcessState)
				}
				t.Logf("process %d complete: %v", i, spp.ProcessState)
			}
			// The deferred inProcPeerWG.Wait() and inProcCancel will take care of shutting down the in-process goroutines.
		})
	}
}
