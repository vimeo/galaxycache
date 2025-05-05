//go:build go1.23

package chtest

import (
	"slices"
	"testing"
)

func TestMapArgs3Peers(t *testing.T) {
	// owners
	const (
		peer1 = "peer1"
		peer2 = "peer2"
		peer3 = "peer3"
	)
	soP1 := SingleOwnerKey(peer1)
	soP2 := SingleOwnerKey(peer2)
	soP3 := SingleOwnerKey(peer3)

	ma := NewMapArgs(Args{
		Owners: []string{peer1, peer2, peer3},
		RegisterKeys: map[string][]string{
			"allpeers":     nil,
			"p1explicit":   {peer1},
			"p2explicit":   {peer2},
			"p1p2explicit": {peer1, peer2},
		},
	})
	if expSegs := 4; ma.NSegsPerKey != expSegs {
		t.Errorf("unexpected number of key segments: %d; expected %d", ma.NSegsPerKey, expSegs)
	}
	// make the full map function-scope so we can log it in error cases for later scopes
	fullMap := ma.NewMap()
	fullMap.Add(peer1, peer2, peer3)
	{
		if soP1Owner := fullMap.Get(soP1); soP1Owner != peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", soP1, soP1Owner, peer1)
		}
		if soP2Owner := fullMap.Get(soP2); soP2Owner != peer2 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", soP2, soP2Owner, peer2)
		}
		if soP3Owner := fullMap.Get(soP3); soP3Owner != peer3 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", soP3, soP3Owner, peer3)
		}
		if explAllOwner := fullMap.Get("allpeers"); explAllOwner != peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", "allpeers", explAllOwner, peer1)
		}
		// fetch with the same number of replicas as peers
		if explAllOwner := fullMap.GetReplicated("allpeers", 3); !slices.Equal(explAllOwner, []string{peer1, peer2, peer3}) {
			t.Errorf("unexpected peer owners for key %q; got %v; want %v", "allpeers", explAllOwner, []string{peer1, peer2, peer3})
		}
		// fetch with more replicas than peers
		if explAllOwner := fullMap.GetReplicated("allpeers", 8); !slices.Equal(explAllOwner, []string{peer1, peer2, peer3}) {
			t.Errorf("unexpected peer owners for key %q; got %v; want %v", "allpeers", explAllOwner, []string{peer1, peer2, peer3})
		}
		if explP1Owner := fullMap.Get("p1explicit"); explP1Owner != peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", "p1explicit", explP1Owner, peer1)
		}
		if explP2Owner := fullMap.Get("p2explicit"); explP2Owner != peer2 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", "p2explicit", explP2Owner, peer2)
		}
		if explP1P2Owner := fullMap.GetReplicated("p1p2explicit", 2); !slices.Equal(explP1P2Owner, []string{peer1, peer2}) {
			t.Errorf("unexpected peer owners for key %q; got %v; want %v", "p1p2explicit", explP1P2Owner, []string{peer1, peer2})
		}
	}
	{
		p2Fallthrough := FallthroughKey(peer1, peer2)
		p3Fallthrough := FallthroughKey(peer1, peer3)
		mapSansPeer1 := ma.NewMap()
		mapSansPeer1.Add(peer2, peer3)
		if soP1Owner := mapSansPeer1.Get(soP1); soP1Owner == peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; wanted something other than %q (excluded from ring)", soP1, soP1Owner, peer1)
		}
		if soP2Owner := mapSansPeer1.Get(soP2); soP2Owner != peer2 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", soP2, soP2Owner, peer2)
		}
		if soP3Owner := mapSansPeer1.Get(soP3); soP3Owner != peer3 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", soP3, soP3Owner, peer3)
		}
		if soP2Owner := mapSansPeer1.Get(p2Fallthrough); soP2Owner != peer2 {
			t.Errorf("unexpected fallthrough peer owner for key %q; got %q; want %q", p2Fallthrough, soP2Owner, peer2)
			t.Logf("sans 1 ring: %+v", mapSansPeer1)
			t.Logf("full ring: %+v", fullMap)
			t.Logf("p2FallThrough hash: %d", ma.HashFunc([]byte(p2Fallthrough)))
		}
		if soP3Owner := mapSansPeer1.Get(p3Fallthrough); soP3Owner != peer3 {
			t.Errorf("unexpected fallthrough peer owner for key %q; got %q; want %q", p3Fallthrough, soP3Owner, peer3)
			t.Logf("sans 1 ring: %+v", mapSansPeer1)
			t.Logf("full ring: %+v", fullMap)
			t.Logf("p3FallThrough hash: %d", ma.HashFunc([]byte(p3Fallthrough)))
		}
	}

}

func TestMapArgs2Peers(t *testing.T) {
	// owners
	const (
		peer1 = "peer1"
		peer2 = "peer2"
	)
	soP1 := SingleOwnerKey(peer1)
	soP2 := SingleOwnerKey(peer2)
	ftP1P2 := FallthroughKey(peer1, peer2)
	ftP2P1 := FallthroughKey(peer2, peer1)

	ma := NewMapArgs(Args{
		Owners: []string{peer1, peer2},
		RegisterKeys: map[string][]string{
			"allpeers":     nil,
			"p1explicit":   {peer1},
			"p2explicit":   {peer2},
			"p1p2explicit": {peer1, peer2},
		},
	})
	if expSegs := 2; ma.NSegsPerKey != expSegs {
		t.Errorf("unexpected number of key segments: %d; expected %d", ma.NSegsPerKey, expSegs)
	}
	// make the full map function-scope so we can log it in error cases for later scopes
	fullMap := ma.NewMap()
	fullMap.Add(peer1, peer2)
	{
		if soP1Owner := fullMap.Get(soP1); soP1Owner != peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", soP1, soP1Owner, peer1)
		}
		if soP2Owner := fullMap.Get(soP2); soP2Owner != peer2 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", soP2, soP2Owner, peer2)
		}
		if soP1Owner := fullMap.Get(ftP1P2); soP1Owner != peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", ftP1P2, soP1Owner, peer1)
		}
		if soP2Owner := fullMap.Get(ftP2P1); soP2Owner != peer2 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", ftP2P1, soP2Owner, peer2)
			t.Logf("full ring: %+v", fullMap)
			t.Logf("p2p1 hash: %d", ma.HashFunc([]byte(ftP1P2)))
		}
		if explAllOwner := fullMap.Get("allpeers"); explAllOwner != peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", "allpeers", explAllOwner, peer1)
		}
		// fetch with the same number of replicas as peers
		if explAllOwner := fullMap.GetReplicated("allpeers", 2); !slices.Equal(explAllOwner, []string{peer1, peer2}) {
			t.Errorf("unexpected peer owners for key %q; got %v; want %v", "allpeers", explAllOwner, []string{peer1, peer2})
		}
		// fetch with more replicas than peers
		if explAllOwner := fullMap.GetReplicated("allpeers", 8); !slices.Equal(explAllOwner, []string{peer1, peer2}) {
			t.Errorf("unexpected peer owners for key %q; got %v; want %v", "allpeers", explAllOwner, []string{peer1, peer2})
		}
		if explP1Owner := fullMap.Get("p1explicit"); explP1Owner != peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", "p1explicit", explP1Owner, peer1)
		}
		if explP2Owner := fullMap.Get("p2explicit"); explP2Owner != peer2 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", "p2explicit", explP2Owner, peer2)
		}
		if explP1P2Owner := fullMap.GetReplicated("p1p2explicit", 2); !slices.Equal(explP1P2Owner, []string{peer1, peer2}) {
			t.Errorf("unexpected peer owners for key %q; got %v; want %v", "p1p2explicit", explP1P2Owner, []string{peer1, peer2})
		}
	}
	{
		ftP1P2 := FallthroughKey(peer1, peer2)
		mapSansPeer1 := ma.NewMap()
		mapSansPeer1.Add(peer2)
		if soP1Owner := mapSansPeer1.Get(soP1); soP1Owner == peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; wanted something other than %q (excluded from ring)", soP1, soP1Owner, peer1)
		}
		if soP2Owner := mapSansPeer1.Get(soP2); soP2Owner != peer2 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", soP2, soP2Owner, peer2)
		}
		if soP2Owner := mapSansPeer1.Get(ftP1P2); soP2Owner != peer2 {
			t.Errorf("unexpected fallthrough peer owner for key %q; got %q; want %q", ftP1P2, soP2Owner, peer2)
			t.Logf("sans 1 ring: %+v", mapSansPeer1)
			t.Logf("full ring: %+v", fullMap)
			t.Logf("p2FallThrough hash: %d", ma.HashFunc([]byte(ftP1P2)))
		}
	}
	{
		mapSansPeer1 := ma.NewMap()
		mapSansPeer1.Add(peer1)
		if soP2Owner := mapSansPeer1.Get(soP2); soP2Owner == peer2 {
			t.Errorf("unexpected peer owner for key %q; got %q; wanted something other than %q (excluded from ring)", soP2, soP2Owner, peer2)
		}
		if soP2Owner := mapSansPeer1.Get(soP2); soP2Owner != peer1 {
			t.Errorf("unexpected peer owner for key %q; got %q; want %q", soP2, soP2Owner, peer1)
		}
		if soP2Owner := mapSansPeer1.Get(ftP2P1); soP2Owner != peer1 {
			t.Errorf("unexpected fallthrough peer owner for key %q; got %q; want %q", ftP2P1, soP2Owner, peer1)
			t.Logf("sans 1 ring: %+v", mapSansPeer1)
			t.Logf("full ring: %+v", fullMap)
			t.Logf("p2FallThrough hash: %d", ma.HashFunc([]byte(ftP2P1)))
		}
	}
}
