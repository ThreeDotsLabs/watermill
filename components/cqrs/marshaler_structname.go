package cqrs

import (
	"fmt"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

// StructNameMarshaler uses the struct name (without the package) as the message name.
//
// It wraps another marshaler which takes care of the message serialization/deserialization.
// If none provided, it falls back to JSON marshaler.
type StructNameMarshaler struct {
	Marshaler CommandEventMarshaler
}

func (m *StructNameMarshaler) marshaler() CommandEventMarshaler {
	if m.Marshaler == nil {
		return JSONMarshaler{}
	}

	return m.Marshaler
}

func (m *StructNameMarshaler) Marshal(v interface{}) (*message.Message, error) {
	msg, err := m.marshaler().Marshal(v)
	if err != nil {
		return nil, err
	}

	msg.Metadata.Set("name", m.Name(v))

	return msg, nil
}

func (m *StructNameMarshaler) Unmarshal(msg *message.Message, v interface{}) error {
	return m.marshaler().Unmarshal(msg, v)
}

func (m *StructNameMarshaler) Name(v interface{}) string {
	segments := strings.Split(fmt.Sprintf("%T", v), ".")

	return segments[len(segments)-1]
}

func (m *StructNameMarshaler) NameFromMessage(msg *message.Message) string {
	return msg.Metadata.Get("name")
}
