package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

const (
	DelayedUntilKey = "delayed_until"
	DelayedForKey   = "delayed_for"
)

type DelayMetadata struct {
	config DelayMetadataConfig
}

type DelayMetadataConfig struct {
	Delay    time.Duration
	MaxDelay time.Duration
}

func (c *DelayMetadataConfig) setDefaults() {
	if c.Delay == 0 {
		c.Delay = 10 * time.Second
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = 160 * time.Second
	}
}

func NewDelayMetadata(config DelayMetadataConfig) *DelayMetadata {
	config.setDefaults()

	return &DelayMetadata{
		config: config,
	}
}

func (d *DelayMetadata) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		msgs, err := h(msg)
		if err != nil {
			d.applyDelay(msg)
		}

		return msgs, err
	}
}

func (d *DelayMetadata) applyDelay(msg *message.Message) {
	delayedForStr := msg.Metadata.Get(DelayedForKey)
	delayedFor, err := time.ParseDuration(delayedForStr)
	if err != nil {
		delayedFor = d.config.Delay
	} else {
		delayedFor = delayedFor * 2
	}

	delayedUntil := time.Now().UTC().Add(delayedFor)
	msg.Metadata.Set(DelayedUntilKey, delayedUntil.Format(time.RFC3339))
	msg.Metadata.Set(DelayedForKey, delayedFor.String())
}
