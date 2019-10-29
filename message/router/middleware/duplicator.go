package middleware

import (
	"errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	notIdempotentError = errors.New("handler is not idempotent")
)

// Duplicator sends message twice.
func Duplicator(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		firstProducedMessages, firstErr := h(msg)
		secondProducedMessages, secondErr := h(msg)
		allErrorsNil := firstErr == nil && secondErr == nil
		allErrorsEqual := firstErr != nil && secondErr != nil && firstErr.Error() == secondErr.Error()

		if allErrorsNil || allErrorsEqual {
			if len(firstProducedMessages) == len(secondProducedMessages) {
				for index, msg := range firstProducedMessages {
					messagesEqual := msg.Equals(secondProducedMessages[index])
					if !messagesEqual {
						return nil, notIdempotentError
					}
				}

				return firstProducedMessages, nil
			}
		}

		return nil, notIdempotentError
	}
}
