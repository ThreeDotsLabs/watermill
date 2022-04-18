package bus

import (
	"context"
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

type JSONMarshaler struct {
	NewUUID      func() string
	GenerateType func(v any) string
}

func (m JSONMarshaler) Marshal(ctx context.Context, v any) (*message.Message, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(
		m.newUUID(),
		b,
	)
	msg.Metadata.Set("type", m.Type(v))

	return msg, nil
}

func (m JSONMarshaler) newUUID() string {
	if m.NewUUID != nil {
		return m.NewUUID()
	}

	// default
	return watermill.NewUUID()
}

func (JSONMarshaler) Unmarshal(msg *message.Message, v any) (err error) {
	return json.Unmarshal(msg.Payload, v)
}

func (m JSONMarshaler) Type(message any) string {
	if m.GenerateType != nil {
		return m.GenerateType(message)
	}

	return StructName(message)
}

func (m JSONMarshaler) TypeFromMessage(msg *message.Message) string {
	return msg.Metadata.Get("type")
}
