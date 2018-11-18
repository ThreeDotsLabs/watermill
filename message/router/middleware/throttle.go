package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Throttle struct {
	perSecond int
	logger    watermill.LoggerAdapter
}

func NewThrottlePerSecond(perSecond int, logger watermill.LoggerAdapter) (Throttle, error) {
	return Throttle{perSecond, logger}, nil
}

func (t Throttle) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		defer func() {
			time.Sleep(time.Duration(int(time.Second) / t.perSecond))
		}()

		return h(message)
	}
}
