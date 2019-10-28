package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// Duplicator sends message twice.
func Duplicator(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		events, err := h(msg)
		_, _ = h(msg)

		return events, err
	}
}
