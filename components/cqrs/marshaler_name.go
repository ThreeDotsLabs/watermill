package cqrs

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// NameMarshaler retrieves the name from a message implementing the following interface:
// 		type namedMessage interface {
// 			Name() string
// 		}
//
// It wraps another marshaler which takes care of the message serialization/deserialization.
// If none provided, it falls back to JSON marshaler.
type NameMarshaler struct {
	Marshaler CommandEventMarshaler
}

func (m *NameMarshaler) marshaler() CommandEventMarshaler {
	if m.Marshaler == nil {
		return JSONMarshaler{}
	}

	return m.Marshaler
}

func (m *NameMarshaler) Marshal(v interface{}) (*message.Message, error) {
	msg, err := m.marshaler().Marshal(v)
	if err != nil {
		return nil, err
	}

	msg.Metadata.Set("name", m.Name(v))

	return msg, nil
}

func (m *NameMarshaler) Unmarshal(msg *message.Message, v interface{}) error {
	return m.marshaler().Unmarshal(msg, v)
}

func (m *NameMarshaler) Name(v interface{}) string {
	if v, ok := v.(namedMessage); ok {
		return v.Name()
	}

	return m.marshaler().Name(v)
}

func (m *NameMarshaler) NameFromMessage(msg *message.Message) string {
	return msg.Metadata.Get("name")
}
