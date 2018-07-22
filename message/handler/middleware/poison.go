package middleware

import (
	"github.com/roblaszczak/gooddd/message/handler"
	"github.com/roblaszczak/gooddd/message"
)

type poisonQueueHook func(message message.Message, err error)

func PoisonQueueHook(hook poisonQueueHook) handler.Middleware {
	return func(h handler.Func) handler.Func {
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
func PoisonKafkaQueue() handler.Middleware {
	return nil
}
