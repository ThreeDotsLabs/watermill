package domain

import (
	"github.com/roblaszczak/gooddd/pubsub"
	"time"
	"fmt"
)

// todo - change to interface?
type Event struct {
	ID         string
	Name       string
	Payload    interface{}
	OccurredOn time.Time

	AggregateVersion int
	AggregateID      string
	AggregateType    string
}

type EventPayload interface {
	AggregateID() string
	AggregateType() string
	AggregateVersion() int
}

type uuidGenerator func() string

type EventsFactory struct {
	generateUUID uuidGenerator
}

func NewEventsFactory(generateUUID uuidGenerator) EventsFactory {
	return EventsFactory{generateUUID}
}

func (e EventsFactory) NewEvent(payload EventPayload) Event {
	return Event{
		ID:         e.generateUUID(),
		Name:       fmt.Sprintf("%T", payload), // todo - do something with it
		Payload:    payload,
		OccurredOn: time.Now(),

		AggregateVersion: payload.AggregateVersion(), // todo - fix it
		AggregateID:      payload.AggregateID(),      // todo - provide by interface of event?
		AggregateType:    payload.AggregateType(),    // todo - provide by payload interface
	}
}

func (e EventsFactory) NewEvents(payloads []EventPayload) []Event {
	var events []Event

	for _, payload := range payloads {
		events = append(events, e.NewEvent(payload))
	}

	return events
}

// todo - validate on commit
// todo - comment
func EventsToMessagePayloads(events []EventPayload) []pubsub.MessagePayload {
	var messages []pubsub.MessagePayload

	for _, event := range events {
		messages = append(messages, event)
	}

	return messages
}
