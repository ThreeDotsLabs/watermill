package domain

import (
	"time"
)

type Event interface {
	EventID() []byte
	EventOccurredOn() time.Time

	AggregateID() []byte
	AggregateType() string
	AggregateVersion() int // todo - make it optional?
}
