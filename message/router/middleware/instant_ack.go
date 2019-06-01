package middleware

import "github.com/ThreeDotsLabs/watermill/message"

// InstantAck makes the handler instantly acknowledge the incoming message, regardless of any errors.
func InstantAck(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		message.Ack()
		return h(message)
	}
}
