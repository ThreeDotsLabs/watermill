package middleware

import (
	"errors"
	"time"

	"github.com/ThreeDotsLabs/watermill"
)

// permanentErrorCause returns the error that err marks as permanent: wrapped with Permanent,
// or with the deprecated backoff.Permanent, which is logged as deprecated.
func permanentErrorCause(err error, logger watermill.LoggerAdapter) (cause error, ok bool) {
	var permanent *PermanentError
	if errors.As(err, &permanent) {
		return permanent.Err, true
	}

	if cause, ok := deprecatedBackoffPermanentError(err); ok {
		logDeprecatedBackoffError(logger, "PermanentError", "Permanent", err)
		return cause, true
	}

	return nil, false
}

// retryAfterDelay returns the delay that err asks for: with RetryAfter,
// or with the deprecated backoff.RetryAfterError, which is logged as deprecated.
func retryAfterDelay(err error, logger watermill.LoggerAdapter) (delay time.Duration, ok bool) {
	var retryAfter *RetryAfterError
	if errors.As(err, &retryAfter) {
		return retryAfter.Delay, true
	}

	if delay, ok := deprecatedBackoffRetryAfterDelay(err); ok {
		logDeprecatedBackoffError(logger, "RetryAfterError", "RetryAfter", err)
		return delay, true
	}

	return 0, false
}

// PermanentError is an error that the Retry middleware does not retry.
// Handlers create it with Permanent.
type PermanentError struct {
	Err error
}

// Permanent wraps err so that the Retry middleware returns it without retrying.
// The middleware returns err itself, without the PermanentError around it,
// and calls none of its hooks for it.
// It returns nil if err is nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

// RetryAfterError is an error after which the Retry middleware waits Delay before the next retry,
// instead of the next delay of its backoff. Handlers create it with RetryAfter.
type RetryAfterError struct {
	Err   error
	Delay time.Duration
}

// RetryAfter wraps err so that the Retry middleware waits delay before the next retry,
// for example the delay that a rate-limited API asked for.
// The backoff then starts again from InitialInterval.
// MaxRetries, MaxElapsedTime and ShouldRetry still apply, and ShouldRetry gets delay.
// It returns nil if err is nil.
func RetryAfter(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	return &RetryAfterError{Err: err, Delay: delay}
}

func (e *RetryAfterError) Error() string {
	return e.Err.Error()
}

func (e *RetryAfterError) Unwrap() error {
	return e.Err
}
