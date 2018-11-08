package domain

type EventProducer struct {
	events []Event
}

func (e *EventProducer) RecordThat(event Event) {
	if event == nil {
		return
	}
	e.events = append(e.events, event)
}

func (e *EventProducer) PopEvents() []Event {
	defer func() { e.events = nil }()
	return e.events
}
