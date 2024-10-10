package delay

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

type contextKey string

var (
	DelayedUntilCtxKey = contextKey("delayed_until")
	DelayedForCtxKey   = contextKey("delayed_for")
)

const (
	DelayedUntilKey = "delayed_until"
	DelayedForKey   = "delayed_for"
)

func Until(msg *message.Message, delayedUntil time.Time) {
	msg.Metadata.Set(DelayedUntilKey, delayedUntil.Format(time.RFC3339))
	msg.Metadata.Set(DelayedForKey, delayedUntil.Sub(time.Now().UTC()).String())
}

func For(msg *message.Message, delayedFor time.Duration) {
	msg.Metadata.Set(DelayedForKey, delayedFor.String())
	msg.Metadata.Set(DelayedUntilKey, time.Now().UTC().Add(delayedFor).Format(time.RFC3339))
}

func UntilWithContext(ctx context.Context, delayedUntil time.Time) context.Context {
	return context.WithValue(ctx, DelayedUntilCtxKey, delayedUntil)
}

func ForWithContext(ctx context.Context, delayedFor time.Duration) context.Context {
	return context.WithValue(ctx, DelayedForCtxKey, delayedFor)
}

type DelayingPublisherDecoratorConfig struct {
	DefaultDelay time.Duration
}

func DelayingPublisherDecorator(pub message.Publisher, config DelayingPublisherDecoratorConfig) (message.Publisher, error) {
	return &delayingPublisher{
		pub:    pub,
		config: config,
	}, nil
}

type delayingPublisher struct {
	pub    message.Publisher
	config DelayingPublisherDecoratorConfig
}

func (p *delayingPublisher) Publish(topic string, messages ...*message.Message) error {
	for i := range messages {
		p.applyDelay(messages[i])
	}
	return p.pub.Publish(topic, messages...)
}

func (p *delayingPublisher) Close() error {
	return p.pub.Close()
}

func (p *delayingPublisher) applyDelay(msg *message.Message) {
	if msg.Metadata.Get(DelayedForKey) != "" {
		return
	}

	if msg.Context().Value(DelayedUntilCtxKey) != nil {
		Until(msg, msg.Context().Value(DelayedUntilCtxKey).(time.Time))
		return
	}

	if msg.Context().Value(DelayedForCtxKey) != nil {
		For(msg, msg.Context().Value(DelayedForCtxKey).(time.Duration))
		return
	}

	if p.config.DefaultDelay > 0 {
		For(msg, p.config.DefaultDelay)
	}
}
