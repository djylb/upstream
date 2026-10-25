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
	// Error numbers are compared as integers, not as error values.
	switch n := uintptr(errno); n {
	case uintptr(syscall.ECONNREFUSED):
		return ReasonRefused, true
	case uintptr(syscall.ETIMEDOUT):
		return ReasonTimeout, true
	case uintptr(syscall.EHOSTUNREACH), uintptr(syscall.ENETUNREACH), uintptr(syscall.ENETDOWN), uintptr(syscall.EADDRNOTAVAIL):
		return ReasonUnreachable, true
	case uintptr(syscall.ECONNRESET), uintptr(syscall.ECONNABORTED):
		return ReasonReset, true
	default:
		if n != 0 && n == errHostDown {
			return ReasonUnreachable, true
		}
	}
	return ReasonNone, false
}
