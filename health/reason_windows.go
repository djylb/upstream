//go:build windows

package health

import (
	"errors"
	"syscall"
)

// Winsock reports socket failures as WSAE* codes; the syscall.E* constants
// on Windows are values the network stack never returns.
const (
	wsaEADDRNOTAVAIL syscall.Errno = 10049
	wsaENETDOWN      syscall.Errno = 10050
	wsaENETUNREACH   syscall.Errno = 10051
	wsaECONNABORTED  syscall.Errno = 10053
	wsaECONNRESET    syscall.Errno = 10054
	wsaETIMEDOUT     syscall.Errno = 10060
	wsaECONNREFUSED  syscall.Errno = 10061
	wsaEHOSTDOWN     syscall.Errno = 10064
	wsaEHOSTUNREACH  syscall.Errno = 10065
)

func errnoReason(err error) (Reason, bool) {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return ReasonNone, false
	}
	switch errno {
	case wsaECONNREFUSED, syscall.ECONNREFUSED:
		return ReasonRefused, true
	case wsaETIMEDOUT, syscall.ETIMEDOUT:
		return ReasonTimeout, true
	case wsaEHOSTUNREACH, wsaENETUNREACH, wsaEHOSTDOWN, wsaENETDOWN, wsaEADDRNOTAVAIL,
		syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.EHOSTDOWN, syscall.ENETDOWN, syscall.EADDRNOTAVAIL:
		return ReasonUnreachable, true
	case wsaECONNRESET, wsaECONNABORTED, syscall.ECONNRESET, syscall.ECONNABORTED:
		return ReasonReset, true
	}
	return ReasonNone, false
}
