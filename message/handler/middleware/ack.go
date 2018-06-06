package middleware

import (
	"github.com/roblaszczak/gooddd/message/handler"
	"github.com/roblaszczak/gooddd/message"
)

// todo - test
func Ack(h handler.HandlerFunc) handler.HandlerFunc {
	return func(msg message.Message) ([]message.Message, error) {
		defer func() {
			msg.Acknowledge()
		}()

		return h(msg)
	}
}
