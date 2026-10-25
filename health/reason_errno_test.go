//go:build !plan9

package health

import (
	"net"
	"os"
	"syscall"
)

var errnoCases = []struct {
	err  error
	want Reason
}{
	{&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}, ReasonRefused},
	{&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNRESET)}, ReasonReset},
	{&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.EHOSTUNREACH)}, ReasonUnreachable},
	{&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ETIMEDOUT)}, ReasonTimeout},
	{&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.Errno(errHostDown))}, ReasonUnreachable},
	{&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ENETDOWN)}, ReasonUnreachable},
	{&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.EADDRNOTAVAIL)}, ReasonUnreachable},
}
