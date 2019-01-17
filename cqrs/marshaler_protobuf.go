package cqrs

import (
	"fmt"
	"reflect"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/gogo/protobuf/proto"
	uuid "github.com/satori/go.uuid"
)

type ProtoBufMarshaler struct {
	NewUUID func() string
}

type NoProtoMessageError struct {
	v interface{}
}

func (e NoProtoMessageError) Error() string {
	rv := reflect.ValueOf(e.v)
	if rv.Kind() != reflect.Ptr {
		return "v is not proto.Message, you must pass pointer value to implement proto.Message"
	}

	return "v is not proto.Message"
}

func (m ProtoBufMarshaler) Marshal(v interface{}) (*message.Message, error) {
	protoMsg, ok := v.(proto.Message)
	if !ok {
		return nil, errors.WithStack(NoProtoMessageError{v})
	}

	b, err := proto.Marshal(protoMsg)
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

func (m ProtoBufMarshaler) newUUID() string {
	if m.NewUUID != nil {
		return m.NewUUID()
	}

	// default
	return uuid.NewV4().String()
}

func (ProtoBufMarshaler) Unmarshal(msg *message.Message, v interface{}) (err error) {
	return proto.Unmarshal(msg.Payload, v.(proto.Message))
}

// todo - benchmark
func (m ProtoBufMarshaler) Name(cmdOrEvent interface{}) string {
	return fmt.Sprintf("%T", cmdOrEvent)
}

func (m ProtoBufMarshaler) MarshaledName(msg *message.Message) string {
	return msg.Metadata.Get("name")
}
