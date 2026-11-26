// Package latency computes moving averages of nonnegative durations.
package latency

import "time"

// Update seeds the average with the first sample. Later samples move it by
// one divisor-th of the difference, rounding toward the existing average.
func Update(old, sample time.Duration, divisor int, first bool) time.Duration {
	if first {
		return sample
	}
	return old + (sample-old)/time.Duration(divisor)
}
