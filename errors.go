package natasks

import "errors"

// ErrNoRetry marks a handler error as non-retriable.
var ErrNoRetry = errors.New("natasks: no retry")

type noRetryError struct {
	err error
}

func (e *noRetryError) Error() string {
	if e.err == nil {
		return ErrNoRetry.Error()
	}

	return e.err.Error()
}

func (e *noRetryError) Unwrap() error {
	return e.err
}

func (e *noRetryError) Is(target error) bool {
	return target == ErrNoRetry
}

// NoRetry wraps err so the worker acknowledges the message without retries or DLQ.
func NoRetry(err error) error {
	if err == nil {
		return nil
	}

	return &noRetryError{err: err}
}

func unwrapNoRetry(err error) error {
	if !errors.Is(err, ErrNoRetry) {
		return err
	}

	unwrapped := errors.Unwrap(err)
	if unwrapped == nil {
		return err
	}

	return unwrapped
}
