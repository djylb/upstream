//go:build !windows && !plan9 && !wasip1

package health

import "syscall"

// errHostDown is EHOSTDOWN where the platform defines it.
const errHostDown = syscall.EHOSTDOWN
