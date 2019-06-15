package middleware

import (
	"time"

	"github.com/cenkalti/backoff"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Retry provides a middleware that retries the handler if errors are returned.
// The retry behaviour is configurable, with exponential backoff and maximum elapsed time.
type Retry struct {
	// MaxRetries is maximum number of times a retry will be attempted.
	MaxRetries int

	// InitalInterval is the first interval between retries. Subsequent intervals will be scaled by Multiplier.
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

	Logger watermill.LoggerAdapter
}

func (r Retry) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		producedMessages, err := h(msg)
		if err == nil {
			return producedMessages, nil
		}

		expBackoff := backoff.NewExponentialBackOff()
		expBackoff.InitialInterval = r.InitialInterval
		expBackoff.MaxInterval = r.MaxInterval
		expBackoff.Multiplier = r.Multiplier
		expBackoff.MaxElapsedTime = r.MaxElapsedTime
		expBackoff.RandomizationFactor = r.RandomizationFactor
		ticker := backoff.NewTicker(expBackoff)
		defer ticker.Stop()

		retryNum := 1
	retryLoop:
		for {
			select {
			case <-msg.Context().Done():
				return producedMessages, err
			case _, ok := <-ticker.C:
				if !ok {
					break retryLoop
				}
			}

			producedMessages, err = h(msg)
			if err == nil {
				return producedMessages, nil
			}

			if r.Logger != nil {
				r.Logger.Error("Error occurred, retrying", err, watermill.LogFields{
					"retry_no":     retryNum,
					"max_retries":  r.MaxRetries,
					"wait_time":    expBackoff.NextBackOff(),
					"elapsed_time": expBackoff.GetElapsedTime(),
				})
			}
			if r.OnRetryHook != nil {
				r.OnRetryHook(retryNum, expBackoff.NextBackOff())
			}

			retryNum++
			if retryNum > r.MaxRetries {
				break retryLoop
			}
		}

		return nil, err
	}
}
