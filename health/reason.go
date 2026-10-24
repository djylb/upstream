package health

import (
	"context"
	"errors"
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
