package domain

import (
	"time"
)

type Event interface {
	OccurredOn() time.Time
	Name() string

	AggregateID() []byte
	AggregateType() string
}

type VersionedEvent interface {
	Event

	AggregateVersion() int
}
