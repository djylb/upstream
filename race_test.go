//go:build race

package upstream

// raceEnabled reports whether the race detector is on; it makes values
// escape that otherwise stay on the stack.
const raceEnabled = true
