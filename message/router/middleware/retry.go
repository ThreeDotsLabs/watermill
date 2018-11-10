package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
)

const RetryForever = -1

type OnRetryHook func(retryNum int, delay time.Duration)

type Retry struct {
	MaxRetries int

	WaitTime time.Duration
	Backoff  int64

	MaxDelay time.Duration

	OnRetryHook OnRetryHook

	Logger watermill.LoggerAdapter
}

func (r Retry) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		retries := 0

		for {
			events, err := h(message)
			if r.shouldRetry(err, retries) {
				waitTime := r.calculateWaitTime()

				if r.Logger != nil {
					r.Logger.Error("Error occurred, retrying", err, watermill.LogFields{
						"retry_no":    retries,
						"max_retries": r.MaxRetries,
						"wait_time":   waitTime,
					})
				}

				retries++
				time.Sleep(waitTime)

				if r.OnRetryHook != nil {
					r.OnRetryHook(retries, r.WaitTime)

				}

				continue
			}

			return events, err
		}
	}
}

func (r Retry) calculateWaitTime() time.Duration {
	waitTime := r.WaitTime + (r.WaitTime * time.Duration(r.Backoff))

	if r.MaxDelay != 0 && waitTime > r.MaxDelay {
		return r.MaxDelay
	}

	return waitTime
}

func (r Retry) shouldRetry(err error, retries int) bool {
	return err != nil && (retries <= r.MaxRetries || r.MaxRetries == RetryForever)
}
