package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
)

type OnRetryHook func(retryNum int, delay time.Duration)

type Retry struct {
	MaxRetries int

	InitialInterval   time.Duration
	MaxInterval       time.Duration
	BackOffMultiplier int
	MaxElapsedTime    time.Duration

	OnRetryHook OnRetryHook

	Logger watermill.LoggerAdapter
}

func (r Retry) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		retries := 0
		startTime := time.Now()

		for {
			events, err := h(message)
			if err == nil {
				return events, nil
			}

			elapsedTime := time.Since(startTime)
			if !r.shouldRetry(retries, elapsedTime) {
				return events, err
			}

			waitTime := r.calculateWaitTime()
			if r.Logger != nil {
				r.Logger.Error("Error occurred, retrying", err, watermill.LogFields{
					"retry_no":     retries,
					"max_retries":  r.MaxRetries,
					"wait_time":    waitTime,
					"elapsed_time": elapsedTime,
				})
			}
			retries++

			select {
			case <-time.After(waitTime):
			// ok
			case <-message.Context().Done():
				return events, err
			}

			if r.OnRetryHook != nil {
				r.OnRetryHook(retries, r.InitialInterval)
			}
		}
	}
}

func (r Retry) calculateWaitTime() time.Duration {
	backOffToAdd := r.InitialInterval * time.Duration(r.BackOffMultiplier)

	waitTime := r.InitialInterval + backOffToAdd

	if r.MaxInterval != 0 && waitTime > r.MaxInterval {
		return r.MaxInterval
	}

	return waitTime
}

func (r Retry) shouldRetry(retries int, elapsedTime time.Duration) bool {
	if r.MaxElapsedTime != 0 && elapsedTime > r.MaxElapsedTime {
		return false
	}

	if r.MaxRetries == 0 {
		return true
	}

	return retries < r.MaxRetries
}
