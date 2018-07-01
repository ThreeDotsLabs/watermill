package domain

import (
	"time"
	"github.com/roblaszczak/gooddd/message"
)

type Event interface {
	message.Message

	OccurredOn() time.Time
	Name() string

	AggregateID() []byte
	AggregateType() string
}

type VersionedEvent interface {
	Event

	AggregateVersion() int
}
