package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Throttle struct {
	PerSecond int
}

func (t Throttle) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		defer func() {
			if t.PerSecond <= 0 {
				return
			}

			time.Sleep(time.Duration(int(time.Second) / t.PerSecond))
		}()

		return h(message)
	}
}
