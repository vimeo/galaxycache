package jsoncodec_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/vimeo/galaxycache"
	"github.com/vimeo/galaxycache/jsoncodec"
)

type testMessage struct {
	Name string
	City string
}

// Test of common-good-case
func TestBackendGetterGood(t *testing.T) {
	beGood := func(ctx context.Context, key string) (testMessage, error) {
		return testMessage{
			Name: "TestName",
			City: "TestCity",
		}, nil
	}

	be := jsoncodec.BackendGetter(beGood)

	ctx := context.Background()

	// test with a proto codec passed (local common-case)
	{
		pc := jsoncodec.Codec[testMessage]{}

		if getErr := be.Get(ctx, "foobar", &pc); getErr != nil {
			t.Errorf("noop Get call failed: %s", getErr)
		}

		pv := pc.Get()
		if pv.City != "TestCity" {
			t.Errorf("unexpected value for City: %v", pv.City)
		}
		if pv.Name != "TestName" {
			t.Errorf("unexpected value for Name: %v", pv.Name)
		}
	}
	// test with a ByteCodec to exercise the common-case when a remote-fetch is done
	{
		c := galaxycache.ByteCodec{}

		if getErr := be.Get(ctx, "foobar", &c); getErr != nil {
			t.Errorf("noop Get call failed: %s", getErr)
		}

		if len(c) < len("TestName")+len("TestCity") {
			t.Errorf("marshaled bytes too short (less than sum of two string fields)")
		}

		pc := jsoncodec.Codec[testMessage]{}

		if umErr := pc.UnmarshalBinary([]byte(c)); umErr != nil {
			t.Errorf("failed to unmarshal bytes: %s", umErr)
		}

		pv := pc.Get()
		if pv.City != "TestCity" {
			t.Errorf("unexpected value for City: %v", pv.City)
		}
		if pv.Name != "TestName" {
			t.Errorf("unexpected value for Name: %v", pv.Name)
		}
	}
}

func TestBackendGetterBad(t *testing.T) {
	sentinel := errors.New("sentinel error")

	beErrorer := func(ctx context.Context, key string) (*testMessage, error) {
		return nil, fmt.Errorf("error: %w", sentinel)
	}

	be := jsoncodec.BackendGetter(beErrorer)

	ctx := context.Background()

	// test with a proto codec passed (local common-case)
	{
		pc := jsoncodec.Codec[testMessage]{}

		if getErr := be.Get(ctx, "foobar", &pc); getErr == nil {
			t.Errorf("noop Get call didn't fail")
		} else if !errors.Is(getErr, sentinel) {
			t.Errorf("Error from Get did not wrap/equal sentinel")
		}
	}
	// test with a ByteCodec to exercise the common-case when a remote-fetch is done
	{
		c := galaxycache.ByteCodec{}

		if getErr := be.Get(ctx, "foobar", &c); getErr == nil {
			t.Errorf("noop Get call didn't fail")
		} else if !errors.Is(getErr, sentinel) {
			t.Errorf("Error from Get did not wrap/equal sentinel")
		}
	}
}
