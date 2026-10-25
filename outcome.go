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

// OutcomeOf maps an error to an Outcome: nil is Success, an error from
// WithOutcome has the outcome given there, an error wrapping
// context.Canceled is Neutral, and any other error is Failure.
// context.DeadlineExceeded is a Failure because a backend that does not
// answer in time is the usual cause.
func OutcomeOf(err error) Outcome {
	if err == nil {
		return Success
	}
	var oe *outcomeError
	switch {
	case errors.As(err, &oe):
		return oe.o
	case errors.Is(err, context.Canceled):
		return Neutral
	default:
		return Failure
	}
}

// WithOutcome returns an error that wraps err and that OutcomeOf maps to o,
// so that a function run by Pool.Do can say how its error reflects on the
// backend: for example Neutral for a request the client got wrong, which
// is neither counted against the backend nor retried. It returns nil if err
// is nil.
func WithOutcome(err error, o Outcome) error {
	if err == nil {
		return nil
	}
	return &outcomeError{err: err, o: o}
}

type outcomeError struct {
	err error
	o   Outcome
}

func (e *outcomeError) Error() string { return e.err.Error() }

func (e *outcomeError) Unwrap() error { return e.err }

// String returns the name of the outcome.
//
//goland:noinspection GoMixedReceiverTypes
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

// MarshalText encodes the outcome as its name.
//
//goland:noinspection GoMixedReceiverTypes
func (o Outcome) MarshalText() ([]byte, error) { return []byte(o.String()), nil }
