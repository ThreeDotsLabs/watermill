package middleware

import (
	"github.com/roblaszczak/gooddd/handler"
	"time"
)

const RetryForever = -1

// todo - pass context/metadata
type OnRetryHook func(retryNum int, delay time.Duration)

// todo - doc
// todo - constructors (infinite retry, backoff (??), etc.)
// todo - tests
// todo - support for invalid messages (to not retry)
type Retry struct {
	MaxRetries int
	//MaxDelay    time.Duration todo
	WaitTime    time.Duration
	OnRetryHook OnRetryHook

	// todo - wait time strategy
}

func NewRetry() *Retry {
	return &Retry{
		MaxRetries: RetryForever,
		WaitTime:   time.Millisecond * 100,
	}
}

func (r Retry) Middleware(h handler.Handler) handler.Handler {
	return func(message handler.Message) ([]handler.MessagePayload, error) {
		retries := 0

		for {
			// todo - what if events aren't empty? global error?
			events, err := h(message)
			if err != nil && (retries <= r.MaxRetries || r.MaxRetries == RetryForever) {
				// todo - move to func
				retries++
				time.Sleep(r.WaitTime)
				r.OnRetryHook(retries, r.WaitTime)
				continue
			}

			return events, err
		}
	}
}
