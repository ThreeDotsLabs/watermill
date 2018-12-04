package abandoned_cart

import (
	"encoding/json"
	"time"
)

type Event interface {
	EventName() string
	EventOccurredOn() time.Time
}

type eventName struct {
	Name string `json:"name"`
}

type eventTransport struct {
	Name       string      `json:"name"`
	OccurredOn time.Time   `json:"occurred_on"`
	Payload    interface{} `json:"payload"`
}

// todo - test
func MarshalEvent(event Event) ([]byte, error) {
	return json.Marshal(eventTransport{
		Name:       event.EventName(),
		OccurredOn: event.EventOccurredOn(),
		Payload:    event,
	})
}

// todo - test
func UnmarshalEvent(data []byte, events []Event) (ok bool, err error) {
	name := eventName{}

	// todo - benchmark that faster to unmarshal 1 value
	if err := json.Unmarshal(data, &name); err != nil {
		return false, err
	}

	for _, event := range events {
		if name.Name == event.EventName() {
			return true, json.Unmarshal(data, event)
		}
	}

	return false, nil
}
