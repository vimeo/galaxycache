package galaxycache

import (
	"testing"
	"time"

	"github.com/vimeo/go-clocks/fake"
)

func TestCapExpiry(t *testing.T) {
	baseTime := time.Now()
	for _, tbl := range []struct {
		name                       string
		ttl                        time.Duration
		jitter                     time.Duration // negative is 0
		inExpiry                   time.Time
		expExpiryMax, expExpiryMin time.Time
	}{
		{
			name: "no_ttl_all_zeroes",
			// all zero-values
		}, {
			name: "no_ttl_negative_ttl",
			ttl:  -3,
		}, {
			name:         "no_ttl_passthrough",
			ttl:          0,
			inExpiry:     baseTime.Add(time.Hour),
			expExpiryMax: baseTime.Add(time.Hour),
			expExpiryMin: baseTime.Add(time.Hour),
		}, {
			name:         "ttl_no_jitter",
			ttl:          time.Hour * 2,
			inExpiry:     time.Time{},
			expExpiryMax: baseTime.Add(time.Hour * 2),
			expExpiryMin: baseTime.Add(time.Hour * 2),
		}, {
			name:         "ttl_no_jitter_set_as_negative",
			ttl:          time.Hour * 2,
			inExpiry:     time.Time{},
			jitter:       -time.Hour,
			expExpiryMax: baseTime.Add(time.Hour * 2),
			expExpiryMin: baseTime.Add(time.Hour * 2),
		}, {
			name:         "ttl_positive_negative",
			ttl:          time.Hour * 2,
			jitter:       time.Minute * 3,
			inExpiry:     time.Time{},
			expExpiryMax: baseTime.Add(time.Hour * 2),
			expExpiryMin: baseTime.Add(time.Hour*2 - time.Minute*3),
		}, {
			name:         "ttl_positive_jitter_reduce_expiry",
			ttl:          time.Hour * 2,
			jitter:       time.Minute * 3,
			inExpiry:     baseTime.Add(time.Hour * 20),
			expExpiryMax: baseTime.Add(time.Hour * 2),
			expExpiryMin: baseTime.Add(time.Hour*2 - time.Minute*3),
		}, {
			name:         "ttl_positive_jitter_don't_reduce_expiry_small",
			ttl:          time.Hour * 2,
			jitter:       time.Minute * 3,
			inExpiry:     baseTime.Add(time.Hour * 1),
			expExpiryMax: baseTime.Add(time.Hour * 1),
			expExpiryMin: baseTime.Add(time.Hour * 1),
		}, {
			name:         "ttl_positive_don't_jitter_reduce_expiry_large",
			ttl:          time.Hour * 2,
			jitter:       -time.Minute * 3,
			inExpiry:     baseTime.Add(time.Hour*2 - time.Nanosecond),
			expExpiryMax: baseTime.Add(time.Hour*2 - time.Nanosecond),
			expExpiryMin: baseTime.Add(time.Hour*2 - time.Nanosecond),
		},
	} {
		t.Run(tbl.name, func(t *testing.T) {
			ttl := newTTLJitter(tbl.ttl, tbl.jitter)
			fc := fake.NewClock(baseTime)

			bi := BackendGetInfo{
				Expiration: tbl.inExpiry,
			}

			ttl.capExpiry(fc, &bi)

			if tbl.expExpiryMax.IsZero() && !bi.Expiration.IsZero() {
				t.Errorf("expiration set %s; with no expected expiration", bi.Expiration)
			} else if bi.Expiration.Before(tbl.expExpiryMin) || bi.Expiration.After(tbl.expExpiryMax) {
				t.Errorf("expiration out of range: got %s; want between %s & %s", bi.Expiration, tbl.expExpiryMin, tbl.expExpiryMax)
			}
		})
	}
}
