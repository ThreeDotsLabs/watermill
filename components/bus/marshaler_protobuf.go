package bus

import (
	"context"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// ProtobufMarshaler is the default Protocol Buffers marshaler.
type ProtobufMarshaler struct {
	NewUUID      func() string
	GenerateType func(v any) string
}

// NoProtoMessageError is returned when the given value does not implement proto.Message.
type NoProtoMessageError struct {
	v any
}

func (e NoProtoMessageError) Error() string {
	rv := reflect.ValueOf(e.v)
	if rv.Kind() != reflect.Ptr {
		return "v is not proto.Message, you must pass pointer value to implement proto.Message"
	}

	return "v is not proto.Message"
}

// Marshal marshals the given protobuf's message into watermill's Message.
func (m ProtobufMarshaler) Marshal(ctx context.Context, v any) (*message.Message, error) {
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
	msg.Metadata.Set("type", m.Type(v))

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
func (ProtobufMarshaler) Unmarshal(msg *message.Message, v any) (err error) {
	return proto.Unmarshal(msg.Payload, v.(proto.Message))
}

func (m ProtobufMarshaler) Type(message any) string {
	if m.GenerateType != nil {
		return m.GenerateType(message)
	}

	return StructName(message)
}

func (m ProtobufMarshaler) TypeFromMessage(msg *message.Message) string {
	return msg.Metadata.Get("type")
}
