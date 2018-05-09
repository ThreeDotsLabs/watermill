package middleware

import (
	"github.com/roblaszczak/gooddd/handler"
)

type poisonQueueHook func(message handler.Message)

func PoisonQueueHook(hook poisonQueueHook) handler.Middleware {
	return func(h handler.Handler) handler.Handler {
		return func(message handler.Message) ([]handler.MessagePayload, error) {
			events, err := h(message)
			if err != nil {
				hook(message)
			}

			return events, err
		}
	}
}

// todo
func PoisonKafkaQueue() handler.Middleware {
	return nil
}
