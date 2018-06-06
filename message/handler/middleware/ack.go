package middleware

import (
	"github.com/roblaszczak/gooddd/message/handler"
	"github.com/roblaszczak/gooddd/message"
)

// todo - test
func AckOnSuccess(h handler.HandlerFunc) handler.HandlerFunc {
	return func(msg message.Message) (_ []message.Message, err error) {
		defer func() {
			if err == nil {
				msg.Acknowledge()
			}
		}()

		return h(msg)
	}
}
