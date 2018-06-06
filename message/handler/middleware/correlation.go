package middleware

// todo - move to separated package?

import (
	"github.com/roblaszczak/gooddd/message/handler"
	"github.com/roblaszczak/gooddd/message"
)

const CorrelationUUIDMetadataKey = "correlation_uuid"

func CorrelationUUID(h handler.HandlerFunc) handler.HandlerFunc {
	return func(message message.Message) ([]message.Message, error) {
		producedMessages, err := h(message)

		correlationUUID := MessageCorrelationUUID(message)
		for _, msg := range producedMessages {
			SetCorrelationUUID(correlationUUID, msg)
		}

		return producedMessages, err
	}
}

func MessageCorrelationUUID(message message.Message) string {
	return message.GetMetadata(CorrelationUUIDMetadataKey)
}

func SetCorrelationUUID(correlationUUID string, msg message.Message) {
	msg.SetMetadata(CorrelationUUIDMetadataKey, correlationUUID)
}
