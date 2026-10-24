package uphttp

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrStatusRange means a status range could not be parsed.
var ErrStatusRange = errors.New("uphttp: invalid status range")

// A StatusRange is an inclusive range of HTTP status codes.
type StatusRange struct {
	Min, Max int
}

// ParseStatusRange parses a status range written as a single code ("204"),
// a class ("2xx") or an inclusive range ("200-399"). Codes must be from 100
// to 999.
func ParseStatusRange(s string) (StatusRange, error) {
	s = strings.TrimSpace(s)
	if len(s) == 3 && (strings.HasSuffix(s, "xx") || strings.HasSuffix(s, "XX")) {
		if s[0] < '1' || s[0] > '9' {
			return StatusRange{}, rangeError(s)
		}
		lo := int(s[0]-'0') * 100
		return StatusRange{Min: lo, Max: lo + 99}, nil
	}
	lo, hi, isRange := strings.Cut(s, "-")
	if !isRange {
		hi = lo
	}
	a, okA := parseCode(strings.TrimSpace(lo))
	b, okB := parseCode(strings.TrimSpace(hi))
	if !okA || !okB || a > b {
		return StatusRange{}, rangeError(s)
	}
	return StatusRange{Min: a, Max: b}, nil
}

func rangeError(s string) error {
	return fmt.Errorf("%w: %q", ErrStatusRange, s)
}

func parseCode(s string) (int, bool) {
	if len(s) != 3 {
		return 0, false
	}
	n := 0
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int(s[i]-'0')
	}
	return n, n >= 100
}

// Contains reports whether code is in the range.
func (r StatusRange) Contains(code int) bool { return code >= r.Min && code <= r.Max }

// String formats the range in the form ParseStatusRange accepts.
func (r StatusRange) String() string {
	if r.Min == r.Max {
		return strconv.Itoa(r.Min)
	}
	return strconv.Itoa(r.Min) + "-" + strconv.Itoa(r.Max)
}
