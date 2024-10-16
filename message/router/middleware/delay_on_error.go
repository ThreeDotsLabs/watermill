package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

// DelayOnError is a middleware that adds the delay metadata to the message if an error occurs.
//
// IMPORTANT: The delay metadata doesn't cause delays with all Pub/Subs! Using it won't have any effect on Pub/Subs that don't support it.
type DelayOnError struct {
	config DelayOnErrorConfig
}

// DelayOnErrorConfig is the configuration for the DelayOnError middleware.
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

// NewDelayOnError creates a new DelayOnError middleware.
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
