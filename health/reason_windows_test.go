package health

import (
	"os"
	"syscall"
	"testing"
)

// errHostDown is the portable EHOSTDOWN value, which errnoReason accepts
// besides WSAEHOSTDOWN.
const errHostDown = uintptr(syscall.EHOSTDOWN)

func TestNetNameDeleted(t *testing.T) {
	if Classify(os.NewSyscallError("read", syscall.ERROR_NETNAME_DELETED)) != ReasonReset {
		t.Fatal("lost reset classification")
	}
}
