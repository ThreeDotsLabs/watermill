package command

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/gogo/protobuf/proto"
	uuid "github.com/satori/go.uuid"
)

type ProtoBufMarshaler struct {
	NewUUID func() string
}

func (m ProtoBufMarshaler) Marshal(cmd interface{}) (*message.Message, error) {
	b, err := proto.Marshal(cmd.(proto.Message))
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

func (m ProtoBufMarshaler) newUUID() string {
	if m.NewUUID != nil {
		return m.NewUUID()
	}

	// default
	return uuid.NewV4().String()
}

func (ProtoBufMarshaler) Unmarshal(msg *message.Message, cmd interface{}) (err error) {
	return proto.Unmarshal(msg.Payload, cmd.(proto.Message))
}

// todo - benchmark
func (m ProtoBufMarshaler) CommandName(cmd interface{}) string {
	return fmt.Sprintf("%T", cmd)
}

func (m ProtoBufMarshaler) MarshaledCommandName(msg *message.Message) string {
	return msg.Metadata.Get("command_name")
}
