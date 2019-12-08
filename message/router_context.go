package message

import (
	"context"
)

type ctxKey string

const (
	handlerNameKey    ctxKey = "handler_name"
	publisherNameKey  ctxKey = "publisher_name"
	subscriberNameKey ctxKey = "subscriber_name"
	subscribeTopicKey ctxKey = "subscribe_topic"
	publishTopicKey   ctxKey = "publish_topic"
)

func valFromCtx(ctx context.Context, key ctxKey) string {
	val, ok := ctx.Value(key).(string)
	if !ok {
		return ""
	}
	return val
}

// HandlerNameFromCtx returns the name of the message handler in the router that consumed the message.
func HandlerNameFromCtx(ctx context.Context) string {
	return valFromCtx(ctx, handlerNameKey)
}

// PublisherNameFromCtx returns the name of the message publisher type that published the message in the router.
// For example, for Kafka it will be `kafka.Publisher`.
func PublisherNameFromCtx(ctx context.Context) string {
	return valFromCtx(ctx, publisherNameKey)
}

// SubscriberNameFromCtx returns the name of the message subscriber type that subscribed to the message in the router.
// For example, for Kafka it will be `kafka.Subscriber`.
func SubscriberNameFromCtx(ctx context.Context) string {
	return valFromCtx(ctx, subscriberNameKey)
}

// SubscribeTopicFromCtx returns the topic from which message was received in the router.
func SubscribeTopicFromCtx(ctx context.Context) string {
	return valFromCtx(ctx, subscribeTopicKey)
}

// PublishTopicFromCtx returns the topic to which message will be published by the router.
func PublishTopicFromCtx(ctx context.Context) string {
	return valFromCtx(ctx, publishTopicKey)
}
