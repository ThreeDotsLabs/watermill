package middleware

import (
	"github.com/roblaszczak/gooddd/message/handler"
	"github.com/roblaszczak/gooddd/message"
)

const CorrelationIDMetadataKey = "correlation_id"

func CorrelationID(h handler.Func) handler.Func {
	return func(message message.ConsumedMessage) ([]message.ProducedMessage, error) {
		producedMessages, err := h(message)

		correlationID := MessageCorrelationID(message)
		for _, msg := range producedMessages {
			SetCorrelationID(correlationID, msg)
		}

		return producedMessages, err
	}
}

func MessageCorrelationID(message message.Message) string {
	return message.GetMetadata(CorrelationIDMetadataKey)
}

func SetCorrelationID(id string, msg message.ProducedMessage) {
	if MessageCorrelationID(msg) != "" {
		return
	}

	msg.SetMetadata(CorrelationIDMetadataKey, id)
}
