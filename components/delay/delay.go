package delay

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
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

func DelayingPublisherDecorator(pub message.Publisher, delay time.Duration) message.Publisher {
	return &delayingPublisher{
		pub:   pub,
		delay: delay,
	}
}

type delayingPublisher struct {
	pub   message.Publisher
	delay time.Duration
}

func (p *delayingPublisher) Publish(topic string, messages ...*message.Message) error {
	for i := range messages {
		For(messages[i], p.delay)
	}
	return p.pub.Publish(topic, messages...)
}

func (p *delayingPublisher) Close() error {
	return p.pub.Close()
}
