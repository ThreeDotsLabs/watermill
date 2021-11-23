package cqrs

import (
	"reflect"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// ProtobufMarshaler is the default Protocol Buffers marshaler.
type ProtobufMarshaler struct {
	NewUUID      func() string
	GenerateName func(v interface{}) string
}

// NoProtoMessageError is returned when the given value does not implement proto.Message.
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

// Marshal marshals the given protobuf's message into watermill's Message.
func (m ProtobufMarshaler) Marshal(v interface{}) (*message.Message, error) {
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

func (m ProtobufMarshaler) newUUID() string {
	if m.NewUUID != nil {
		return m.NewUUID()
	}

	// default
	return watermill.NewUUID()
}

// Unmarshal unmarshals given watermill's Message into protobuf's message.
func (ProtobufMarshaler) Unmarshal(msg *message.Message, v interface{}) (err error) {
	return proto.Unmarshal(msg.Payload, v.(proto.Message))
}

// Name returns the command or event's name.
func (m ProtobufMarshaler) Name(cmdOrEvent interface{}) string {
	if m.GenerateName != nil {
		return m.GenerateName(cmdOrEvent)
	}

	return FullyQualifiedStructName(cmdOrEvent)
}

// NameFromMessage returns the metadata name value for a given Message.
func (m ProtobufMarshaler) NameFromMessage(msg *message.Message) string {
	return msg.Metadata.Get("name")
}
