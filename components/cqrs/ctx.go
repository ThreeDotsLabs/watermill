package cqrs

import (
	"context"

	"github.com/ascendsoftware/watermill/message"
)

type ctxKey string

const (
	originalMessage ctxKey = "original_message"
)

// OriginalMessageFromCtx returns the original message that was received by the event/command handler.
func OriginalMessageFromCtx(ctx context.Context) *message.Message {
	val, ok := ctx.Value(originalMessage).(*message.Message)
	if !ok {
		return nil
	}
	return val
}

// CtxWithOriginalMessage returns a new context with the original message attached.
func CtxWithOriginalMessage(ctx context.Context, msg *message.Message) context.Context {
	return context.WithValue(ctx, originalMessage, msg)
}
