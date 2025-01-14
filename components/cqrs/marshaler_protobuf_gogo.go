package cqrs

import (
	"fmt"
	"runtime/debug"

	stderrors "errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	stdproto "google.golang.org/protobuf/proto"
)

// ProtobufMarshaler a protobuf marshaler using github.com/gogo/protobuf/proto (deprecated).
//
// DEPRECATED: Use ProtoMarshaler instead. This marshaler will not work with newer protobuf files.
// IMPORTANT: This marshaler is backward and forward compatible with ProtoMarshaler.
// ProtobufMarshaler from Watermill versions until v1.4.3 are not forward compatible with ProtoMarshaler.
// Suggested migration steps:
//  1. Update Watermill to v1.4.4 or newer, so all publishers and subscribers will be forward and backward compatible.
//  2. Change all usages of ProtobufMarshaler to ProtoMarshaler.
type ProtobufMarshaler struct {
	NewUUID      func() string
	GenerateName func(v interface{}) string

	// DisableStdProtoFallback disables fallback to github.com/golang/protobuf/proto when github.com/gogo/protobuf/proto
	// because receiving a message that was marshaled with github.com/golang/protobuf/proto.
	// Fallback is enabled by default to enable migration to ProtoMarshaler.
	DisableStdProtoFallback bool
}

// Marshal marshals the given protobuf's message into watermill's Message.
func (m ProtobufMarshaler) Marshal(v interface{}) (msg *message.Message, err error) {
	defer func() {
		// gogo proto can panic on unmarshal (for example, because it received a message from ProtoMarshaler with oneof)
		if r := recover(); r != nil {
			err = stderrors.Join(err, fmt.Errorf(
				"github.com/gogo/protobuf/proto panic (we recommend migrating marshaler to cqrs.ProtoMarshaler to avoid that): %v\n%s",
				r,
				string(debug.Stack()),
			))
		}

		if err != nil && !m.DisableStdProtoFallback {
			_, isStdProtoMsg := v.(stdproto.Message)

			if isStdProtoMsg {
				msg, err = m.ToProtoMarshaler().Marshal(v)
			}
		}
	}()

	protoMsg, ok := v.(proto.Message)
	if !ok {
		return nil, errors.WithStack(NoProtoMessageError{v})
	}

	b, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, err
	}

	msg = message.NewMessage(
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
func (m ProtobufMarshaler) Unmarshal(msg *message.Message, v interface{}) (err error) {
	protoV, ok := v.(proto.Message)
	if !ok {
		return errors.WithStack(NoProtoMessageError{v})
	}

	defer func() {
		// gogo proto can panic on unmarshal (for example, because it received a message from ProtoMarshaler with oneof)
		if r := recover(); r != nil {
			err = stderrors.Join(err, fmt.Errorf(
				"github.com/gogo/protobuf/proto panic (we recommend migrating marshaler to cqrs.ProtoMarshaler to avoid that): %v\n%s",
				r,
				string(debug.Stack()),
			))
		}

		if err != nil && !m.DisableStdProtoFallback {
			err = m.ToProtoMarshaler().Unmarshal(msg, v)
		}
	}()

	return proto.Unmarshal(msg.Payload, protoV)
}

func (m ProtobufMarshaler) ToProtoMarshaler() ProtoMarshaler {
	return ProtoMarshaler{
		NewUUID:      m.NewUUID,
		GenerateName: m.GenerateName,
	}
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
