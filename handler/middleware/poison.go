package middleware

import (
	"github.com/roblaszczak/gooddd/handler"
	"github.com/roblaszczak/gooddd/domain"
)

type poisonQueueHook func(event domain.Event)

func PoisonQueueHook(hook poisonQueueHook) handler.Middleware {
	return func(h handler.Handler) handler.Handler {
		return func(event domain.Event) ([]domain.EventPayload, error) {
			events, err := h(event)
			if err != nil {
				hook(event)
			}

			return events, err
		}
	}
}

// todo
func PoisonKafkaQueue() handler.Middleware {
	return nil
}
