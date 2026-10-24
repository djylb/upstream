//go:build plan9

package health

var errnoCases []struct {
	err  error
	want Reason
}
