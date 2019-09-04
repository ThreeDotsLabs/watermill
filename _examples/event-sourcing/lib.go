package main

type Event interface{}

type EventProducer struct {
	version int64
	events  []Event
}

func (e *EventProducer) RecordThat(event Event) {
	if event == nil {
		return
	}
	e.events = append(e.events, event)
}

// todo - test without pitor
// todo - does we need pop?
func (e *EventProducer) PopNewEvents() []Event {
	defer func() { e.events = nil }()
	return e.events
}
