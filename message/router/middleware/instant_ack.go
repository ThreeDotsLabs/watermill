package middleware

import "github.com/ThreeDotsLabs/watermill/message"

func InstantAck(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		message.Ack()
		return h(message)
	}
}
