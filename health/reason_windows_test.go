package health

import "syscall"

// errHostDown is the portable EHOSTDOWN value, which errnoReason accepts
// besides WSAEHOSTDOWN.
const errHostDown = syscall.EHOSTDOWN
