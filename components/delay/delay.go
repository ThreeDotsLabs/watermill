package delay

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Delay struct {
	time     time.Time
	duration time.Duration
}

func (d Delay) IsZero() bool {
	return d.time.IsZero()
}

func Until(delayedUntil time.Time) Delay {
	return Delay{
		time:     delayedUntil,
		duration: delayedUntil.Sub(time.Now().UTC()),
	}
}

func For(delayedFor time.Duration) Delay {
	return Delay{
		time:     time.Now().UTC().Add(delayedFor),
		duration: delayedFor,
	}
}

type contextKey string

var (
	delayCtxKey = contextKey("delay")
)

const (
	DelayedUntilKey = "delayed_until"
	DelayedForKey   = "delayed_for"
)

func WithContext(ctx context.Context, delay Delay) context.Context {
	return context.WithValue(ctx, delayCtxKey, delay)
}

func Message(msg *message.Message, delay Delay) {
	msg.Metadata.Set(DelayedUntilKey, delay.time.Format(time.RFC3339))
	msg.Metadata.Set(DelayedForKey, delay.duration.String())
}

type PublisherConfig struct {
	DefaultDelay Delay
}

func NewPublisher(pub message.Publisher, config PublisherConfig) (message.Publisher, error) {
	return &publisher{
		pub:    pub,
		config: config,
	}, nil
}

type publisher struct {
	pub    message.Publisher
	config PublisherConfig
}

func (p *publisher) Publish(topic string, messages ...*message.Message) error {
	for i := range messages {
		p.applyDelay(messages[i])
	}
	return p.pub.Publish(topic, messages...)
}

func (p *publisher) Close() error {
	return p.pub.Close()
}

func (p *publisher) applyDelay(msg *message.Message) {
	if msg.Metadata.Get(DelayedForKey) != "" {
		return
	}

	if msg.Context().Value(delayCtxKey) != nil {
		delay := msg.Context().Value(delayCtxKey).(Delay)
		Message(msg, delay)
		return
	}

	if !p.config.DefaultDelay.IsZero() {
		Message(msg, p.config.DefaultDelay)
	}
}
