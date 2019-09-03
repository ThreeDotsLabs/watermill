package cqrs

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type JSONMarshaler struct {
	NewUUID      func() string
	GenerateName func(v interface{}) string
}

func (m JSONMarshaler) Marshal(v interface{}) (*message.Message, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(
		m.newUUID(),
		b,
	)
	msg.Metadata.Set("name", m.Name(v))

	return msg, nil
}

func (m JSONMarshaler) newUUID() string {
	if m.NewUUID != nil {
		return m.NewUUID()
	}

	// default
	return watermill.NewUUID()
}

func (JSONMarshaler) Unmarshal(msg *message.Message, v interface{}) (err error) {
	return json.Unmarshal(msg.Payload, v)
}

func (m JSONMarshaler) Name(cmdOrEvent interface{}) string {
	if m.GenerateName != nil {
		return m.GenerateName(cmdOrEvent)
	}

	return FullyQualifiedStructName(cmdOrEvent)
}

func (m JSONMarshaler) NameFromMessage(msg *message.Message) string {
	return msg.Metadata.Get("name")
}
