package latency

import (
	"testing"
	"time"
)

func TestUpdate(t *testing.T) {
	for _, tc := range []struct {
		div  int
		want time.Duration
	}{{4, 18}, {8, 19}} {
		avg := Update(0, 20, tc.div, true)
		if got := Update(avg, 11, tc.div, false); got != tc.want {
			t.Fatalf("divisor %d: %v", tc.div, got)
		}
	}
}
