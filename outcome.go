package upstream

import (
	"context"
	"errors"
	"strconv"
)

// Outcome classifies a finished request or connection for passive health
// checking.
type Outcome uint8

const (
	// Success means the backend served the request. It resets the
	// consecutive-failure count.
	Success Outcome = iota

	// Failure means the backend failed the request: it refused the
	// connection, timed out or answered with an error the caller blames on
	// the backend.
	Failure

	// Neutral means the result says nothing about the backend, for example
	// because the client went away. It counts towards neither successes nor
	// failures.
	Neutral
)

// OutcomeOf maps an error to an Outcome: nil is Success, an error wrapping
// context.Canceled is Neutral, and any other error is Failure.
// context.DeadlineExceeded is a Failure because a backend that does not
// answer in time is the usual cause.
func OutcomeOf(err error) Outcome {
	switch {
	case err == nil:
		return Success
	case errors.Is(err, context.Canceled):
		return Neutral
	default:
		return Failure
	}
}

// String returns the name of the outcome.
func (o Outcome) String() string {
	switch o {
	case Success:
		return "success"
	case Failure:
		return "failure"
	case Neutral:
		return "neutral"
	}
	return "Outcome(" + strconv.Itoa(int(o)) + ")"
}
