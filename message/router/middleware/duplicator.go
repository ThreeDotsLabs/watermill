package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// Duplicator sends message twice.
func Duplicator(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		producedMessages, firstErr := h(msg)
		if firstErr != nil {
			return nil, firstErr
		}

		_, secondErr := h(msg)
		if secondErr != nil {
			return nil, secondErr
		}

		return producedMessages, nil
	}
}
