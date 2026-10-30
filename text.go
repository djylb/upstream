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
	return 0, fmt.Errorf("upstream: unknown %s %q", what, s)
}

// UnmarshalText decodes a state from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (s *State) UnmarshalText(b []byte) (err error) {
	*s, err = parseName(string(b), StateRemoved+1, "state")
	return err
}

// UnmarshalText decodes a verdict from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (h *Health) UnmarshalText(b []byte) (err error) {
	*h, err = parseName(string(b), HealthDown+1, "health")
	return err
}

// UnmarshalText decodes a state from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (s *OutlierState) UnmarshalText(b []byte) (err error) {
	*s, err = parseName(string(b), OutlierHalfOpen+1, "outlier state")
	return err
}

// UnmarshalText decodes an outcome from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (o *Outcome) UnmarshalText(b []byte) (err error) {
	*o, err = parseName(string(b), Neutral+1, "outcome")
	return err
}

// UnmarshalText decodes a kind from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (k *EventKind) UnmarshalText(b []byte) (err error) {
	*k, err = parseName(string(b), EventBalancerPanic+1, "event kind")
	return err
}

// UnmarshalText decodes a fallback from its name, as ParseFallback does.
//
//goland:noinspection GoMixedReceiverTypes
func (f *Fallback) UnmarshalText(b []byte) (err error) {
	*f, err = ParseFallback(string(b))
	return err
}
