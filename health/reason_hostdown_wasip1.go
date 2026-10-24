//go:build wasip1

package health

import "syscall"

// errHostDown is zero: WASI defines no EHOSTDOWN.
const errHostDown syscall.Errno = 0
