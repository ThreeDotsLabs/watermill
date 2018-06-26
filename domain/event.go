package domain

import (
	"time"
	"github.com/roblaszczak/gooddd/message"
)

type Event interface {
	message.Message

	EventOccurredOn() time.Time
	EventName() string

	AggregateID() []byte
	AggregateType() string
	AggregateVersion() int
}
