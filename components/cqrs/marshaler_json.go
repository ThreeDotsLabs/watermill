package cqrs

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
)

type JsonMarshaler struct {
	NewUUID func() string
}

func (m JsonMarshaler) Marshal(v interface{}) (*message.Message, error) {
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

func (m JsonMarshaler) newUUID() string {
	if m.NewUUID != nil {
		return m.NewUUID()
	}

	// default
	return watermill.NewUUID()
}

func (JsonMarshaler) Unmarshal(msg *message.Message, v interface{}) (err error) {
	return json.Unmarshal(msg.Payload, v)
}

func (m JsonMarshaler) Name(cmdOrEvent interface{}) string {
	return ObjectName(cmdOrEvent)
}

func (m JsonMarshaler) MarshaledName(msg *message.Message) string {
	return msg.Metadata.Get("name")
}
