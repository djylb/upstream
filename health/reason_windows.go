//go:build windows

package health

import (
	"errors"
	"syscall"
)

// Winsock reports socket failures as WSAE* codes; the syscall.E* constants
// on Windows are values the network stack never returns.
const (
	wsaEADDRNOTAVAIL uintptr = 10049
	wsaENETDOWN      uintptr = 10050
	wsaENETUNREACH   uintptr = 10051
	wsaECONNABORTED  uintptr = 10053
	wsaECONNRESET    uintptr = 10054
	wsaETIMEDOUT     uintptr = 10060
	wsaECONNREFUSED  uintptr = 10061
	wsaEHOSTDOWN     uintptr = 10064
	wsaEHOSTUNREACH  uintptr = 10065
)

func errnoReason(err error) (Reason, bool) {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return ReasonNone, false
	}
	// Error numbers are compared as integers, not as error values.
	switch uintptr(errno) {
	case wsaECONNREFUSED, uintptr(syscall.ECONNREFUSED):
		return ReasonRefused, true
	case wsaETIMEDOUT, uintptr(syscall.ETIMEDOUT):
		return ReasonTimeout, true
	case wsaEHOSTUNREACH, wsaENETUNREACH, wsaEHOSTDOWN, wsaENETDOWN, wsaEADDRNOTAVAIL,
		uintptr(syscall.EHOSTUNREACH), uintptr(syscall.ENETUNREACH), uintptr(syscall.EHOSTDOWN), uintptr(syscall.ENETDOWN), uintptr(syscall.EADDRNOTAVAIL):
		return ReasonUnreachable, true
	case wsaECONNRESET, wsaECONNABORTED, uintptr(syscall.ECONNRESET), uintptr(syscall.ECONNABORTED):
		return ReasonReset, true
	}
	return ReasonNone, false
}
