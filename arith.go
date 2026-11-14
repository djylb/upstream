package upstream

// The conversions below are the places where the package narrows or changes
// the sign of a number it has already bounded. Each one names the bound, so
// that the call sites do not repeat it and the linter's overflow check is
// silenced once, here.

// u64 converts a length, count or index, which is never negative.
func u64(n int) uint64 {
	return uint64(n) //nolint:gosec // n is a length or count, never negative
}

// index converts a value below a length back to an int.
func index(v uint64) int {
	return int(v) //nolint:gosec // v is below a length, so it fits an int
}

// i32 converts a candidate index or a count clamped to the int32 range.
func i32(n int) int32 {
	return int32(n) //nolint:gosec // n is an index below the candidate count or a clamped count
}

// u32 converts a non-negative int32.
func u32(n int32) uint32 {
	return uint32(n) //nolint:gosec // n is a clamped, non-negative setting
}

// low32 returns the low 32 bits of v; the high half is a different field.
func low32(v uint64) uint32 {
	return uint32(v) //nolint:gosec // truncation to the low half is the point
}
