package delay

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Delay represents a message's delay.
// It can be either a delay until a specific time or a delay for a specific duration.
// The zero value of Delay is a zero delay.
//
// IMPORTANT: Delay doesn't work with all Pub/Subs! Using it won't have any effect on Pub/Subs that don't support it.
// See the list of supported Pub/Subs in the documentation: https://watermill.io/advanced/delayed-messages/
type Delay struct {
	time     time.Time
	duration time.Duration
}

func (d Delay) IsZero() bool {
	return d.time.IsZero()
}

// Until returns a delay of the given time.
func Until(delayedUntil time.Time) Delay {
	return Delay{
		time:     delayedUntil,
		duration: delayedUntil.Sub(time.Now().UTC()),
	}
}

// For returns a delay of now plus the given duration.
func For(delayedFor time.Duration) Delay {
	return Delay{
		time:     time.Now().UTC().Add(delayedFor),
		duration: delayedFor,
	}
}

type contextKey string

var (
	delayContextKey = contextKey("delay")
)

// WithContext returns a new context with the given delay.
// If used together with a publisher wrapped with NewPublisher, the delay will be applied to the message.
//
// IMPORTANT: Delay doesn't work with all Pub/Subs! Using it won't have any effect on Pub/Subs that don't support it.
// See the list of supported Pub/Subs in the documentation: https://watermill.io/advanced/delayed-messages/
func WithContext(ctx context.Context, delay Delay) context.Context {
	return context.WithValue(ctx, delayContextKey, delay)
}

const (
	DelayedUntilKey = "_watermill_delayed_until"
	DelayedForKey   = "_watermill_delayed_for"
)

// Message sets the delay metadata on the message.
//
// IMPORTANT: Delay doesn't work with all Pub/Subs! Using it won't have any effect on Pub/Subs that don't support it.
// See the list of supported Pub/Subs in the documentation: https://watermill.io/advanced/delayed-messages/
func Message(msg *message.Message, delay Delay) {
	msg.Metadata.Set(DelayedUntilKey, delay.time.Format(time.RFC3339))
	msg.Metadata.Set(DelayedForKey, delay.duration.String())
}
