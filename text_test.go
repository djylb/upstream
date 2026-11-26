package upstream

import (
	"encoding"
	"encoding/json"
	"testing"
)

func TestFallbackJSONRoundTrip(t *testing.T) {
	type cfg struct{ Fallback Fallback }
	b, err := json.Marshal(cfg{FallbackAll})
	if err != nil {
		t.Fatal(err)
	}
	var c cfg
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatalf("%s does not decode: %v", b, err)
	}
}

func TestInvalidEnumTextPreservesValue(t *testing.T) {
	s, h, o, r, k, f := StateOffline, HealthDown, Failure, OutlierHalfOpen, EventRemoved, FallbackAll
	for _, v := range []encoding.TextUnmarshaler{&s, &h, &o, &r, &k, &f} {
		if v.UnmarshalText([]byte("invalid")) == nil {
			t.Fatal("accepted invalid text")
		}
	}
	if s != StateOffline || h != HealthDown || o != Failure || r != OutlierHalfOpen || k != EventRemoved || f != FallbackAll {
		t.Fatal("invalid text changed the receiver")
	}
}
