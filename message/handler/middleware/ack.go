package middleware

import (
	"github.com/roblaszczak/gooddd/message/handler"
	"github.com/roblaszczak/gooddd/message"
)

// todo - test
func Ack(h handler.HandlerFunc) handler.HandlerFunc {
	return func(event *message.Message) ([]message.Payload, error) {
		events, err := h(event)

		// todo - fix?
		// todo - add multiple options?
		//if err == nil {
			event.Acknowledge()
		//}

		return events, err
	}
}
