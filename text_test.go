package upstream

import (
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
