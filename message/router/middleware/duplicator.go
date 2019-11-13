package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// Duplicator sends message twice.
func Duplicator(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		firstProducedMessages, firstErr := h(msg)
		if firstErr != nil {
			return nil, firstErr
		}

		secondProducedMessages, secondErr := h(msg)
		if secondErr != nil {
			return nil, secondErr
		}

		return append(firstProducedMessages, secondProducedMessages...), nil
	}
}
