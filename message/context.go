package message

import (
	"context"
)

type ctxKey string

const (
	handlerNameKey    ctxKey = "handler_name"
	publisherNameKey  ctxKey = "publisher_name"
	subscriberNameKey ctxKey = "subscriber_name"
)

func valFromCtx(ctx context.Context, key ctxKey) string {
	val, ok := ctx.Value(key).(string)
	if !ok {
		return ""
	}
	return val
}

func HandlerNameFromCtx(ctx context.Context) string {
	return valFromCtx(ctx, handlerNameKey)
}

func PublisherNameFromCtx(ctx context.Context) string {
	return valFromCtx(ctx, publisherNameKey)
}

func SubscriberNameFromCtx(ctx context.Context) string {
	return valFromCtx(ctx, subscriberNameKey)
}
