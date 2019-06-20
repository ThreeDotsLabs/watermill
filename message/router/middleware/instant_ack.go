package middleware

import "github.com/ThreeDotsLabs/watermill/message"

// InstantAck makes the handler instantly acknowledge the incoming message, regardless of any errors.
// It may be used to gain throughput, but at a cost:
// If you had exactly-once delivery, you may expect at-least-once instead.
// If you had ordered messages, the ordering might be broken.
func InstantAck(h message.HandlerFunc) message.HandlerFunc {
	return func(message *message.Message) ([]*message.Message, error) {
		message.Ack()
		return h(message)
	}
}
