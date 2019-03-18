package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

const CorrelationIDMetadataKey = "correlation_id"

func CorrelationID(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		producedMessages, err := h(message)

		correlationID := MessageCorrelationID(message)
		for _, msg := range producedMessages {
			SetCorrelationID(correlationID, msg)
		}

		return producedMessages, err
	}
}

func CorrelationIDWithAutogenerate(generateFunc func() string) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(message *message.Message) ([]*message.Message, error) {
			producedMessages, err := h(message)

			correlationID := MessageCorrelationID(message)
			if correlationID == "" {
				correlationID = generateFunc()
			}

			for _, msg := range producedMessages {
				SetCorrelationID(correlationID, msg)
			}

			return producedMessages, err
		}
	}
}

func MessageCorrelationID(message *message.Message) string {
	return message.Metadata.Get(CorrelationIDMetadataKey)
}

func SetCorrelationID(id string, msg *message.Message) {
	if MessageCorrelationID(msg) != "" {
		return
	}

	msg.Metadata.Set(CorrelationIDMetadataKey, id)
}
