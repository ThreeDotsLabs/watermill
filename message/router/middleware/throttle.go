package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Throttle provides a middleware that limits the amount of messages processed per unit of time.
// This may be done e.g. to prevent excessive load caused by running a handler on a long queue of unprocessed messages.
type Throttle struct {
	ticker *time.Ticker
}

// NewThrottle creates a new Throttle middleware.
// Example duration and count: NewThrottle(10, time.Second) for 10 messages per second
func NewThrottle(count int64, duration time.Duration) *Throttle {
	return &Throttle{
		ticker: time.NewTicker(duration / time.Duration(count)),
	}
}

// Middleware returns the Throttle middleware.
func (t Throttle) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		// throttle is shared by multiple handlers, which will wait for their "tick"
		<-t.ticker.C

		return h(message)
	}
}
