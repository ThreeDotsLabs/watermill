package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// todo - test
func AckOnSuccess(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (_ []*message.Message, err error) {
		defer func() {
			if err == nil {
				msg.Ack()
			}
		}()

		return h(msg)
	}
}
