package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/sony/gobreaker"
)

// CircuitBreaker is a middleware that wraps the handler in a circuit breaker.
// Based on the configuration, the circuit breaker will fail fast if the handler keeps returning errors.
// This is useful for preventing cascading failures.
type CircuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

// NewCircuitBreaker returns a new CircuitBreaker middleware.
// Refer to the gobreaker documentation for the available settings.
func NewCircuitBreaker(settings gobreaker.Settings) CircuitBreaker {
	return CircuitBreaker{
		cb: gobreaker.NewCircuitBreaker(settings),
	}
}

// Middleware returns the CircuitBreaker middleware.
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
