//go:build plan9

package health

// errnoReason finds nothing: Plan 9 reports network failures as strings,
// which Classify does not parse.
func errnoReason(error) (Reason, bool) { return ReasonNone, false }
