package upstream

import "fmt"

// enum is a named integer type whose values from zero up to a count have
// distinct names.
type enum interface {
	~uint8 | ~uint32
	String() string
}

// parseName returns the value of E below n whose name is s.
func parseName[E enum](s string, n E, what string) (E, error) {
	for v := E(0); v < n; v++ {
		if v.String() == s {
			return v, nil
		}
	}
	return 0, fmt.Errorf("upstream: unknown %s %q", what, s) //nolint:err113 // a parse error names the text it rejected
}

func decodeName[E enum](dst *E, b []byte, n E, what string) error {
	v, err := parseName(string(b), n, what)
	if err == nil {
		*dst = v
	}
	return err
}

// UnmarshalText decodes a state from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (s *State) UnmarshalText(b []byte) error {
	return decodeName(s, b, StateOffline+1, "state")
}

// UnmarshalText decodes a verdict from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (h *Health) UnmarshalText(b []byte) error {
	return decodeName(h, b, HealthDown+1, "health")
}

// UnmarshalText decodes a state from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (s *OutlierState) UnmarshalText(b []byte) error {
	return decodeName(s, b, OutlierHalfOpen+1, "outlier state")
}

// UnmarshalText decodes an outcome from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (o *Outcome) UnmarshalText(b []byte) error {
	return decodeName(o, b, Degraded+1, "outcome")
}

// UnmarshalText decodes a kind from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (k *EventKind) UnmarshalText(b []byte) error {
	return decodeName(k, b, EventBalancerPanic+1, "event kind")
}

// UnmarshalText decodes a fallback from its name, as ParseFallback does.
//
//goland:noinspection GoMixedReceiverTypes
func (f *Fallback) UnmarshalText(b []byte) error {
	return decodeName(f, b, FallbackAll+1, "fallback")
}
