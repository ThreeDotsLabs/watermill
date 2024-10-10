package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

type DelayOnError struct {
	config DelayOnErrorConfig
}

type DelayOnErrorConfig struct {
	Delay    time.Duration
	MaxDelay time.Duration
}

func (c *DelayOnErrorConfig) setDefaults() {
	if c.Delay == 0 {
		c.Delay = 10 * time.Second
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = 160 * time.Second
	}
}

func NewDelayOnError(config DelayOnErrorConfig) *DelayOnError {
	config.setDefaults()

	return &DelayOnError{
		config: config,
	}
}

func (d *DelayOnError) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		msgs, err := h(msg)
		if err != nil {
			d.applyDelay(msg)
		}

		return msgs, err
	}
}

func (d *DelayOnError) applyDelay(msg *message.Message) {
	delayedForStr := msg.Metadata.Get(delay.DelayedForKey)
	delayedFor, err := time.ParseDuration(delayedForStr)
	if err != nil {
		delayedFor = d.config.Delay
	} else {
		delayedFor = delayedFor * 2
	}

	delay.Message(msg, delay.For(delayedFor))
}
