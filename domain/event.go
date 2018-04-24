package domain

import (
	"github.com/roblaszczak/gooddd/pubsub"
	"time"
	"github.com/satori/go.uuid"
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

func NewEvent(payload EventPayload) Event {
	return Event{
		ID:         uuid.NewV4().String(),      // todo - inject it
		Name:       fmt.Sprintf("%T", payload), // todo - do something with it
		Payload:    payload,
		OccurredOn: time.Now(),

		AggregateVersion: payload.AggregateVersion(), // todo - fix it
		AggregateID:      payload.AggregateID(),      // todo - provide by interface of event?
		AggregateType:    payload.AggregateType(),    // todo - provide by payload interface
	}
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
