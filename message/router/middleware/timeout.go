package middleware

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Timeout makes the handler cancel the incoming message's context after a specified time.
// Any timeout-sensitive functionality of the handler should listen on msg.Context().Done() to know when to fail.
func Timeout(timeout time.Duration) func(message.HandlerFunc) message.HandlerFunc {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			ctx, cancel := context.WithTimeout(msg.Context(), timeout)
			defer func() {
				cancel()
			}()

			msg.SetContext(ctx)
			return h(msg)
		}
	}
}
