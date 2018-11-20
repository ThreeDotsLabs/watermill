package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Throttle struct {
	throttle <-chan time.Time
}

// NewThrottle creates new Throttle instance.
// Example rate: 10/time.Second
func NewThrottle(rate time.Duration) *Throttle {
	return &Throttle{time.Tick(rate)}
}

func (t Throttle) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		select {
		case <-t.throttle:
			// throttle is shared by multiple handlers, which will wait for their "tick"
		}

		return h(message)
	}
}
