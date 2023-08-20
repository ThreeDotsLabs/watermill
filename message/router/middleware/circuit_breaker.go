package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/sony/gobreaker"
)

type CircuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

func NewCircuitBreaker(settings gobreaker.Settings) CircuitBreaker {
	return CircuitBreaker{
		cb: gobreaker.NewCircuitBreaker(settings),
	}
}

func (c CircuitBreaker) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		out, err := c.cb.Execute(func() (interface{}, error) {
			return h(msg)
		})

		var result []*message.Message
		if out != nil {
			result = out.([]*message.Message)
		}

		return result, err
	}
}
