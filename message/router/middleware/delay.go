package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Delay struct {
	config DelayConfig
}

type DelayConfig struct {
	Delay    time.Duration
	MaxDelay time.Duration
}

func (c *DelayConfig) setDefaults() {
	if c.Delay == 0 {
		c.Delay = 10 * time.Second
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = 160 * time.Second
	}
}

func NewDelay(config DelayConfig) *Delay {
	config.setDefaults()

	return &Delay{
		config: config,
	}
}

func (d *Delay) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		msgs, err := h(msg)
		if err != nil {
			d.applyDelay(msg)
		}

		return msgs, err
	}
}

func (d *Delay) applyDelay(msg *message.Message) {
	delayedForStr := msg.Metadata.Get(delay.DelayedForKey)
	delayedFor, err := time.ParseDuration(delayedForStr)
	if err != nil {
		delayedFor = d.config.Delay
	} else {
		delayedFor = delayedFor * 2
	}

	delayedUntil := time.Now().UTC().Add(delayedFor)
	msg.Metadata.Set(delay.DelayedUntilKey, delayedUntil.Format(time.RFC3339))
	msg.Metadata.Set(delay.DelayedForKey, delayedFor.String())
}
