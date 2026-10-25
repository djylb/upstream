package health

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
)

// ErrBadResponse is wrapped by errors of probes that reached the target but
// rejected its answer; Classify maps it to ReasonResponse.
var ErrBadResponse = errors.New("health: unexpected response")

// Classify returns the Reason for a check error. It inspects error values
// and system error numbers, never error text.
func Classify(err error) Reason {
	if err == nil {
		return ReasonNone
	}
	if errors.Is(err, ErrBadResponse) {
		return ReasonResponse
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return ReasonDNS
	}
	if r, ok := errnoReason(err); ok {
		return r
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return ReasonTimeout
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ReasonTimeout
	}
	return ReasonOther
}

// String returns the name of the reason.
//
//goland:noinspection GoMixedReceiverTypes
func (r Reason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonRefused:
		return "refused"
	case ReasonTimeout:
		return "timeout"
	case ReasonUnreachable:
		return "unreachable"
	case ReasonDNS:
		return "dns"
	case ReasonReset:
		return "reset"
	case ReasonResponse:
		return "response"
	case ReasonOther:
		return "other"
	}
	return "Reason(" + strconv.Itoa(int(r)) + ")"
}

// MarshalText encodes the reason as its name.
//
//goland:noinspection GoMixedReceiverTypes
func (r Reason) MarshalText() ([]byte, error) { return []byte(r.String()), nil }

// UnmarshalText decodes a reason from its name.
//
//goland:noinspection GoMixedReceiverTypes
func (r *Reason) UnmarshalText(b []byte) error {
	for v := ReasonNone; v <= ReasonOther; v++ {
		if v.String() == string(b) {
			*r = v
			return nil
		}
	}
	return fmt.Errorf("health: unknown reason %q", b)
}
