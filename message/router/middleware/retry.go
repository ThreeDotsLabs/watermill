package middleware

import (
	"context"
	"time"

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
//
// A handler can return an error wrapped with Permanent to stop the retries,
// or with RetryAfter to choose the delay before the next retry.
//
// The error types of github.com/cenkalti/backoff/v5, which this middleware used before,
// still work the same way: a handler can return backoff.Permanent(err) or a *backoff.RetryAfterError.
// This is deprecated and will be removed in the next major version; use Permanent and RetryAfter instead.
// The middleware logs an error each time a handler returns one of them.
type Retry struct {
	// MaxRetries is maximum number of times a retry will be attempted.
	// If negative, the number of retries is not limited.
	MaxRetries int

	// InitialInterval is the first interval between retries. Subsequent intervals will be scaled by Multiplier.
	InitialInterval time.Duration
	// MaxInterval sets the limit for the exponential backoff of retries. The interval will not be increased beyond MaxInterval.
	// If 0, the interval is not increased: every interval is InitialInterval.
	MaxInterval time.Duration
	// Multiplier is the factor by which the waiting interval will be multiplied between retries.
	// If 0, the interval is not increased: every interval is InitialInterval.
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
	// This is called when MaxRetries or MaxElapsedTime is reached and the handler still returns an error,
	// and when the message's context is canceled before the handler succeeded.
	// It is NOT called when ShouldRetry returns false.
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
		ctx := msg.Context()
		startedAt := time.Now()

		delays := newExponentialBackoff(r)

		// retriesDone is the number of retries already made, 0 during the first attempt.
		retriesDone := 0
		// waited is the delay waited before the current attempt, 0 for the first one.
		var waited time.Duration

		// attempts includes the first attempt, so it is MaxRetries + 1 when the retries ran out.
		exhausted := func(producedMessages []*message.Message, err error, attempts int) ([]*message.Message, error) {
			if r.OnRetriesExhausted != nil {
				r.OnRetriesExhausted(RetriesExhaustedParams{
					Err:      err,
					RetryNum: attempts,
				})
			}
			return producedMessages, err
		}

		for {
			if ctx.Err() != nil {
				return exhausted(nil, context.Cause(ctx), retriesDone)
			}

			if r.ResetContextOnRetry {
				// message is passed as a pointer, so its context can be canceled
				// by the previous attempts -> it will break retries, because any
				// underlying logic that relies on the context will fail.
				// see more: https://github.com/ThreeDotsLabs/watermill/issues/467
				//
				// to avoid this, we need to reset the original context on each attempt
				// we may lose context value that was set by the previous attempt
				msg.SetContext(ctx)
			}

			producedMessages, err := h(msg)
			if err == nil {
				return producedMessages, nil
			}

			if cause, ok := permanentErrorCause(err, r.Logger); ok {
				return producedMessages, cause
			}

			if r.OnRetryHook != nil && retriesDone > 0 {
				r.OnRetryHook(retriesDone, waited)
			}

			// next advances the backoff, so it is called at most once per attempt:
			// this delay is given to ShouldRetry and then waited.
			var delay time.Duration
			if retryAfter, ok := retryAfterDelay(err, r.Logger); ok {
				delay = retryAfter
				delays.reset()
			} else {
				delay = delays.next()
			}

			nextRetry := retriesDone + 1

			// a negative MaxRetries means no limit
			if r.MaxRetries >= 0 && nextRetry > r.MaxRetries {
				return exhausted(producedMessages, err, retriesDone+1)
			}
			if r.MaxElapsedTime > 0 && time.Since(startedAt)+delay > r.MaxElapsedTime {
				return exhausted(producedMessages, err, retriesDone+1)
			}
			if ctx.Err() != nil {
				return exhausted(producedMessages, context.Cause(ctx), retriesDone+1)
			}

			if r.ShouldRetry != nil && !r.ShouldRetry(RetryParams{
				RetryNum: nextRetry,
				Err:      err,
				Delay:    delay,
			}) {
				return producedMessages, err
			}

			if r.Logger != nil {
				r.Logger.Error("Error occurred, retrying", err, watermill.LogFields{
					"retry_no":    nextRetry,
					"max_retries": r.MaxRetries,
					"wait_time":   delay,
				})
			}

			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return exhausted(producedMessages, context.Cause(ctx), retriesDone+1)
			}

			retriesDone = nextRetry
			waited = delay
		}
	}
}
