package domain

type EventProducer struct {
	events  []EventPayload
}

func (e *EventProducer) RecordThat(event EventPayload) {
	e.events = append(e.events, event)
}

func (e *EventProducer) PopEvents() []EventPayload {
	defer func() { e.events = []EventPayload{} }()
	return e.events
}
