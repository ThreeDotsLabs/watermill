package middleware

import (
	"github.com/roblaszczak/gooddd/message"
)

// todo - test
func AckOnSuccess(h message.HandlerFunc) message.HandlerFunc {
	return func(msg message.ConsumedMessage) (_ []message.ProducedMessage, err error) {
		defer func() {
			if err == nil {
				msg.Acknowledge()
			}
		}()

		return h(msg)
	}
}
