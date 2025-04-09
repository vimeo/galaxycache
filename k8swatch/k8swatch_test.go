//go:build go1.21

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

package k8swatch

import (
	"context"
	"fmt"
	"maps"
	"net"
	"slices"
	"sync"
	"testing"

	k8score "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	testcore "k8s.io/client-go/testing"

	"github.com/vimeo/galaxycache"
	"github.com/vimeo/k8swatcher"
)

const testPodNamespace = "testnamespace"

// creates a pod for testing purposes. Takes in the pod name, hostname,
// the labels, and whether the pod should be ready to service requests
func newTestPod(podIP string, labels map[string]string,
	podReady k8score.ConditionStatus) *k8score.Pod {
	return &k8score.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Labels:    labels,
			Name:      fmt.Sprintf("gc-%s", podIP),
			Namespace: testPodNamespace,
		},
		Status: k8score.PodStatus{
			Conditions: []k8score.PodCondition{
				{
					Type:   k8score.PodReady,
					Status: podReady,
				}},
			PodIP: podIP,
			Phase: k8score.PodRunning,
		},
	}
}

type recordingNullFetchFetchArgs struct {
	peerID string
	galaxy string
	key    string
}

type recordingNullFetchProtocol struct {
	fetchCalls []recordingNullFetchFetchArgs
	knownPeers map[string]struct{}

	mu sync.Mutex // really only called in one goroutine, but not worth the annoyance to rely on
}

func (r *recordingNullFetchProtocol) clone() *recordingNullFetchProtocol {
	return &recordingNullFetchProtocol{
		fetchCalls: slices.Clone(r.fetchCalls),
		knownPeers: maps.Clone(r.knownPeers),
	}
}

// NewFetcher instantiates the connection between the current and a
// remote peer and returns a RemoteFetcher to be used for fetching
// data from that peer
func (r *recordingNullFetchProtocol) NewFetcher(url string) (galaxycache.RemoteFetcher, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.knownPeers[url] = struct{}{}
	return &recordingNullFetcher{
		p:   r,
		url: url,
	}, nil
}

func (r *recordingNullFetchProtocol) removeURL(url string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.knownPeers, url)
}

func (r *recordingNullFetchProtocol) appendArgs(p recordingNullFetchFetchArgs) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fetchCalls = append(r.fetchCalls, p)
}

type recordingNullFetcher struct {
	p   *recordingNullFetchProtocol
	url string
}

func (r *recordingNullFetcher) Fetch(context context.Context, galaxy string, key string) ([]byte, error) {
	r.p.appendArgs(recordingNullFetchFetchArgs{
		peerID: r.url,
		galaxy: galaxy,
		key:    key,
	})
	return nil, fmt.Errorf("noop for galaxy %s; key %s", galaxy, key)
}

// Close closes a client-side connection (may be a nop)
func (r *recordingNullFetcher) Close() error {
	r.p.removeURL(r.url)
	return nil
}

func TestUpdateCB(t *testing.T) {
	// create a fake watcher for tests
	watcher := watch.NewFake()

	selfPodIP := "127.0.0.1"

	selfPod := newTestPod(selfPodIP, map[string]string{"app": "testGalaxycache"}, k8score.ConditionFalse)
	someOtherReadyPod := newTestPod("10.20.30.42", map[string]string{"app": "testGalaxycache"}, k8score.ConditionTrue)

	// initialize a kubernetesClient struct for the tests
	clientset := fake.NewSimpleClientset(selfPod, someOtherReadyPod)
	clientset.PrependWatchReactor("pods", testcore.DefaultWatchReactor(watcher, nil))

	fp := recordingNullFetchProtocol{
		fetchCalls: []recordingNullFetchFetchArgs{},
		knownPeers: map[string]struct{}{},
	}
	selfHP := net.JoinHostPort(selfPodIP, "8080")
	universe := galaxycache.NewUniverse(&fp, selfHP)

	const portNum = 20124

	// Setup a channel to notify us when the callback has run.
	k8sEventCh := make(chan struct{}, 1)

	uCB := UpdateCB(universe, portNum)
	pw := k8swatcher.NewPodWatcher(clientset, testPodNamespace, "app=testGalaxycache", func(ctx context.Context, event k8swatcher.PodEvent) {
		defer func() { k8sEventCh <- struct{}{} }()
		t.Logf("event type %[1]T: %+[1]v", event) // log the events to make it easier to debug
		uCB(ctx, event)
	})

	pwCtx, pwCancel := context.WithCancel(context.Background())
	defer pwCancel()

	doneCh := make(chan struct{}) // closed when done
	go func() {
		defer close(doneCh)
		if pwErr := pw.Run(pwCtx); pwErr != nil {
			t.Errorf("podwatcher failed: %s", pwErr)
		}
	}()

	// The initial dump should include two CreatePod and one InitialListComplete
	<-k8sEventCh
	<-k8sEventCh
	<-k8sEventCh
	{
		f := fp.clone()

		if expCalls := []recordingNullFetchFetchArgs{}; !slices.Equal(f.fetchCalls, expCalls) {
			t.Errorf("unexpected fetchCalls: got %v; want %v", f.fetchCalls, expCalls)
		}
		if expURLs := map[string]struct{}{"10.20.30.42:20124": {}}; !maps.Equal(f.knownPeers, expURLs) {
			t.Errorf("unexpected knownPeers: got %v; want %v", f.knownPeers, expURLs)
		}
	}
	g := universe.NewGalaxy("fimbat", 10, galaxycache.GetterFunc(func(ctx context.Context, key string, dest galaxycache.Codec) error {
		return fmt.Errorf("failed to fetch key %q: unimplemented", key)
	}))

	universe.SetIncludeSelf(false) // make sure we fall through to the remote peer
	bcCodec := galaxycache.ByteCodec{}
	{
		getErr := g.Get(context.Background(), "fooblebit", &bcCodec) // make a request that'll fail in both places
		t.Log(getErr)
		f := fp.clone()

		if expCalls := []recordingNullFetchFetchArgs{{peerID: "10.20.30.42:20124", galaxy: g.Name(), key: "fooblebit"}}; !slices.Equal(f.fetchCalls, expCalls) {
			t.Errorf("unexpected fetchCalls: got %v; want %v", f.fetchCalls, expCalls)
		}
		if expURLs := map[string]struct{}{"10.20.30.42:20124": {}}; !maps.Equal(f.knownPeers, expURLs) {
			t.Errorf("unexpected knownPeers: got %v; want %v", f.knownPeers, expURLs)
		}
	}

	watcher.Delete(someOtherReadyPod)
	<-k8sEventCh

	{
		f := fp.clone()

		if expCalls := []recordingNullFetchFetchArgs{{peerID: "10.20.30.42:20124", galaxy: g.Name(), key: "fooblebit"}}; !slices.Equal(f.fetchCalls, expCalls) {
			t.Errorf("unexpected fetchCalls: got %v; want %v", f.fetchCalls, expCalls)
		}
		if expURLs := map[string]struct{}{}; !maps.Equal(f.knownPeers, expURLs) {
			t.Errorf("unexpected knownPeers: got %v; want %v", f.knownPeers, expURLs)
		}
	}

	// Now we can add/remove pods at will
	watcher.Add(selfPod)
	<-k8sEventCh
	{
		f := fp.clone()

		if expCalls := []recordingNullFetchFetchArgs{{peerID: "10.20.30.42:20124", galaxy: g.Name(), key: "fooblebit"}}; !slices.Equal(f.fetchCalls, expCalls) {
			t.Errorf("unexpected fetchCalls: got %v; want %v", f.fetchCalls, expCalls)
		}
		if expURLs := map[string]struct{}{}; !maps.Equal(f.knownPeers, expURLs) {
			t.Errorf("unexpected knownPeers: got %v; want %v", f.knownPeers, expURLs)
		}
	}

	watcher.Add(someOtherReadyPod)
	<-k8sEventCh
	{
		f := fp.clone()

		if expCalls := []recordingNullFetchFetchArgs{{peerID: "10.20.30.42:20124", galaxy: g.Name(), key: "fooblebit"}}; !slices.Equal(f.fetchCalls, expCalls) {
			t.Errorf("unexpected fetchCalls: got %v; want %v", f.fetchCalls, expCalls)
		}
		if expURLs := map[string]struct{}{"10.20.30.42:20124": {}}; !maps.Equal(f.knownPeers, expURLs) {
			t.Errorf("unexpected knownPeers: got %v; want %v", f.knownPeers, expURLs)
		}
	}

	pwCancel()
	<-doneCh
}
