package middleware

import (
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// RetryParams holds the parameters for a retry attempt
type RetryParams struct {
	// Err is the error that caused the retry attempt.
	Err error
	// RetryNum is the number of the retry attempt, starting from 1.
	RetryNum int
	// Delay is the delay for the next retry attempt.
	Delay time.Duration
}

// RetriesExhaustedParams holds the parameters passed to OnRetriesExhausted.
type RetriesExhaustedParams struct {
	// Err is the last error returned by the handler before retries were exhausted.
	Err error
	// RetryNum is the total number of attempts performed (1 initial + MaxRetries retries).
	// For MaxRetries=N this will equal N+1.
	RetryNum int
}

// Retry provides a middleware that retries the handler if errors are returned.
// The retry behaviour is configurable, with exponential backoff and maximum elapsed time.
type Retry struct {
	// MaxRetries is maximum number of times a retry will be attempted.
	MaxRetries int

	// InitialInterval is the first interval between retries. Subsequent intervals will be scaled by Multiplier.
	InitialInterval time.Duration
	// MaxInterval sets the limit for the exponential backoff of retries. The interval will not be increased beyond MaxInterval.
	MaxInterval time.Duration
	// Multiplier is the factor by which the waiting interval will be multiplied between retries.
	Multiplier float64
	// MaxElapsedTime sets the time limit of how long retries will be attempted. Disabled if 0.
	MaxElapsedTime time.Duration
	// RandomizationFactor randomizes the spread of the backoff times within the interval of:
	// [currentInterval * (1 - RandomizationFactor), currentInterval * (1 + RandomizationFactor)].
	RandomizationFactor float64

	// OnRetryHook is an optional function that will be executed on each retry attempt.
	// The number of the current retry is passed as retryNum,
	OnRetryHook func(retryNum int, delay time.Duration)

	// OnRetriesExhausted is an optional function that will be executed when all retries are exhausted.
	// This is called when MaxRetries is reached and the handler still returns an error.
	// It is NOT called when ShouldRetry returns false (that path returns a permanent error and exits earlier).
	OnRetriesExhausted func(params RetriesExhaustedParams)

	// ShouldRetry is an optional function that will be executed before each retry attempt.
	// If ShouldRetry returns false, the retry will not be attempted.
	ShouldRetry func(params RetryParams) bool

	// ResetContextOnRetry indicates whether the message context should be reset on each retry attempt.
	// See more: https://github.com/ThreeDotsLabs/watermill/issues/467
	//
	// This is not enabled by default to keep backward compatibility
	// (in theory, someone may want to preserve context values between retries).
	ResetContextOnRetry bool

	Logger watermill.LoggerAdapter
}

// Middleware returns the Retry middleware.
func (r Retry) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		originalCtx := msg.Context()
		retryNum := 0
		// stoppedByPermanent is set when ShouldRetry returns false, so we know
		// to skip OnRetriesExhausted — retries weren't actually exhausted, they
		// were short-circuited. backoff/v5 strips the *PermanentError wrapper
		// inside Retry, so we can't detect this from the returned error alone.
		stoppedByPermanent := false

		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.InitialInterval = r.InitialInterval
		expBackoff.MaxInterval = r.MaxInterval
		expBackoff.Multiplier = r.Multiplier
		expBackoff.RandomizationFactor = r.RandomizationFactor

		// MaxRetries + 1 because the first attempt is not a retry
		retryBackoff := backoff.WithMaxTries(uint(r.MaxRetries + 1))

		maxElapsedBackoff := backoff.WithMaxElapsedTime(r.MaxElapsedTime)

		// notification is called on a failed retry attempt.
		notification := func(err error, delay time.Duration) {
			if r.Logger != nil {
				r.Logger.Error("Error occurred, retrying", err, watermill.LogFields{
					"retry_no":    retryNum,
					"max_retries": r.MaxRetries,
					"wait_time":   delay,
				})
			}
		}

		// operation: the function that will be retried.
		operation := func() ([]*message.Message, error) {
			select {
			case <-originalCtx.Done():
				return nil, originalCtx.Err()
			default:
				if r.ResetContextOnRetry {
					// message is passed as a pointer, so its context can be canceled
					// by the previous attempts -> it will break retries, because any
					// underlying logic that relies on the context will fail.
					// see more: https://github.com/ThreeDotsLabs/watermill/issues/467
					//
					// to avoid this, we need to reset the original context on each attempt
					// we may lose context value that was set by the previous attempt
					msg.SetContext(originalCtx)
				}

				producedMessages, err := h(msg)
				if err == nil {
					return producedMessages, nil
				}

				if r.ShouldRetry != nil && !r.ShouldRetry(RetryParams{
					RetryNum: retryNum,
					Err:      err,
					Delay:    expBackoff.NextBackOff(),
				}) {
					// backoff.Permanent will stop the retry attempts
					stoppedByPermanent = true
					return producedMessages, backoff.Permanent(err)
				}

				if r.OnRetryHook != nil && retryNum > 0 {
					// call RetryHook function on each retry attempt.
					r.OnRetryHook(retryNum, expBackoff.NextBackOff())
				}
				retryNum++
				return producedMessages, err
			}
		}

		producedMessages, retryErr := backoff.Retry(
			originalCtx,
			operation,
			backoff.WithBackOff(expBackoff),
			retryBackoff,
			maxElapsedBackoff,
			backoff.WithNotify(notification),
		)
		var backoffPermanentError *backoff.PermanentError
		if errors.As(retryErr, &backoffPermanentError) {
			// just in case, we don't want to expose backoff.PermanentError to the outside world
			return producedMessages, backoffPermanentError.Unwrap()
		}
		if retryErr != nil {
			if r.OnRetriesExhausted != nil && !stoppedByPermanent {
				r.OnRetriesExhausted(RetriesExhaustedParams{
					Err:      retryErr,
					RetryNum: retryNum,
				})
			}
			return producedMessages, retryErr
		}

		return producedMessages, nil
	}
}
