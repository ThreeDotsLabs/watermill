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
	// [currentInterval * (1 - randomization_factor), currentInterval * (1 + randomization_factor)].
	RandomizationFactor float64

	// OnRetryHook is an optional function that will be executed on each retry attempt.
	// The number of the current retry is passed as retryNum,
	OnRetryHook func(retryNum int, delay time.Duration)

	// ShouldRetry is an optional function that will be executed before each retry attempt.
	// If ShouldRetry returns false, the retry will not be attempted.
	ShouldRetry func(params RetryParams) bool

	Logger watermill.LoggerAdapter
}

// Middleware returns the Retry middleware.
func (r Retry) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		retryNum := 0

		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.InitialInterval = r.InitialInterval
		expBackoff.MaxInterval = r.MaxInterval
		expBackoff.Multiplier = r.Multiplier
		expBackoff.RandomizationFactor = r.RandomizationFactor

		// MaxRetries + 1 because the first attempt is not a retry
		retryBackoff := backoff.WithMaxTries(uint(r.MaxRetries + 1))

		maxElapsedBackoff := backoff.WithMaxElapsedTime(r.MaxElapsedTime)

		ctx := msg.Context()

		// notification: called on a failed retry attempt.
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
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
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
			ctx,
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
			return producedMessages, retryErr
		}

		return producedMessages, nil
	}
}
