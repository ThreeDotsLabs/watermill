package command

import (
	"encoding/json"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/satori/go.uuid"
)

type JsonMarshaler struct {
	NewUUID func() string
}

func (m JsonMarshaler) Marshal(cmd interface{}) (*message.Message, error) {
	b, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(
		m.newUUID(),
		b,
	)
	msg.Metadata.Set("command_name", m.CommandName(cmd))

	return msg, nil
}

func (m JsonMarshaler) newUUID() string {
	if m.NewUUID != nil {
		return m.NewUUID()
	}

	// default
	return uuid.NewV4().String()
}

func (JsonMarshaler) Unmarshal(msg *message.Message, cmd interface{}) (err error) {
	return json.Unmarshal(msg.Payload, cmd)
}

// todo - benchmark
func (m JsonMarshaler) CommandName(cmd interface{}) string {
	return fmt.Sprintf("%T", cmd)
}

func (m JsonMarshaler) MarshaledCommandName(msg *message.Message) string {
	return msg.Metadata.Get("command_name")
}
