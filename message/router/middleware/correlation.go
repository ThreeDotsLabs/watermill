package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// CorrelationIDMetadataKey is used to store the correlation ID in metadata.
const CorrelationIDMetadataKey = "correlation_id"

// SetCorrelationID sets a correlation ID for the message.
//
// SetCorrelationID should be called when the message enters the system.
// When message is produced in a request (for example HTTP),
// message correlation ID should be the same as the request's correlation ID.
func SetCorrelationID(id string, msg *message.Message) {
	if MessageCorrelationID(msg) != "" {
		return
	}

	msg.Metadata.Set(CorrelationIDMetadataKey, id)
}

// MessageCorrelationID returns correlation ID from the message.
func MessageCorrelationID(message *message.Message) string {
	return message.Metadata.Get(CorrelationIDMetadataKey)
}

// CorrelationID adds correlation ID to all messages produced by the handler.
// ID is based on ID from message received by handler.
//
// To make CorrelationID working correctly, SetCorrelationID must be called to first message entering the system.
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
