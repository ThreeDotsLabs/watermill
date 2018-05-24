package domain

import (
	"time"
)

type Event interface {
	EventOccurredOn() time.Time

	AggregateID() []byte
	AggregateType() string
	AggregateVersion() int // todo - make it optional?
}
