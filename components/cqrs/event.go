package cqrs

type Event interface {
	EventName() string
}

func EventName(v any) string {
	e := v.(Event)
	return e.EventName()
}
