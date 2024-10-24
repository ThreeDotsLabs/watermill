package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
)

// DelayOnError is a middleware that adds the delay metadata to the message if an error occurs.
//
// IMPORTANT: The delay metadata doesn't cause delays with all Pub/Subs! Using it won't have any effect on Pub/Subs that don't support it.
// See the list of supported Pub/Subs in the documentation: https://watermill.io/advanced/delayed-messages/
type DelayOnError struct {
	// InitialInterval is the first interval between retries. Subsequent intervals will be scaled by Multiplier.
	InitialInterval time.Duration
	// MaxInterval sets the limit for the exponential backoff of retries. The interval will not be increased beyond MaxInterval.
	MaxInterval time.Duration
	// Multiplier is the factor by which the waiting interval will be multiplied between retries.
	Multiplier float64
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
	if delayedForStr != "" && err == nil {
		delayedFor *= time.Duration(d.Multiplier)
		if delayedFor > d.MaxInterval {
			delayedFor = d.MaxInterval
		}

		delay.Message(msg, delay.For(delayedFor))
	} else {
		delay.Message(msg, delay.For(d.InitialInterval))
	}
}
