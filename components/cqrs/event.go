package cqrs

// Event is an interface that describes an event.
type Event interface {
	// EventName should return the name of the event. For example, it could be used for topic generation.
	EventName() string
}

// GenerateEventName returns the Event's name.
// It can be used with marshalers if events implement the Event interface.
func GenerateEventName(v any) string {
	e := v.(Event)
	return e.EventName()
}
