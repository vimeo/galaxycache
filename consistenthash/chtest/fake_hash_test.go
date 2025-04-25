//go:build go1.23

package chtest

import "testing"

func TestMapArgs(t *testing.T) {
	// owners
	const (
		peer1 = "peer1"
		peer2 = "peer2"
		peer3 = "peer3"
	)
	soP1 := SingleOwnerKey(peer1)
	soP2 := SingleOwnerKey(peer2)
	soP3 := SingleOwnerKey(peer3)

	ma := NewMapArgs(Args{Owners: []string{peer1, peer2, peer3}})
	if expSegs := 2; ma.NSegsPerKey != expSegs {
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
