//go:build !windows && !plan9

package health

import (
	"errors"
	"syscall"
)

func errnoReason(err error) (Reason, bool) {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return ReasonNone, false
	}
	switch errno {
	case syscall.ECONNREFUSED:
		return ReasonRefused, true
	case syscall.ETIMEDOUT:
		return ReasonTimeout, true
	case syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.ENETDOWN, syscall.EADDRNOTAVAIL:
		return ReasonUnreachable, true
	case syscall.ECONNRESET, syscall.ECONNABORTED:
		return ReasonReset, true
	}
	if errno != 0 && errno == errHostDown {
		return ReasonUnreachable, true
	}
	return ReasonNone, false
}
