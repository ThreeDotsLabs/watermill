package middleware

import (
	"github.com/roblaszczak/gooddd/message"
)

type poisonQueueHook func(message message.Message, err error)

func PoisonQueueHook(hook poisonQueueHook) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(message message.ConsumedMessage) ([]message.ProducedMessage, error) {
			events, err := h(message)
			if err != nil {
				hook(message, err)
			}

			return events, err
		}
	}
}

// todo
func PoisonKafkaQueue() message.HandlerFunc {
	return nil
}
