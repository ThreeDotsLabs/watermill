package eventsourcing

import "time"

type Event interface {
	EventName() string
	EventOccurredOn() time.Time
}

type EventHandler interface {
	HandleEvent(Event) error
}
