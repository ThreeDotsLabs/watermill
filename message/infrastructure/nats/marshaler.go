package nats

import (
	"bytes"
	"encoding/gob"

	stan "github.com/nats-io/stan.go"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Marshaler interface {
	Marshal(topic string, msg *message.Message) ([]byte, error)
}

type Unmarshaler interface {
	Unmarshal(*stan.Msg) (*message.Message, error)
}

type MarshalerUnmarshaler interface {
	Marshaler
	Unmarshaler
}

// GobMarshaler is marshaller which is using Gob to marshal Watermill messages.
type GobMarshaler struct{}

func (GobMarshaler) Marshal(topic string, msg *message.Message) ([]byte, error) {
	// todo - use pool
	buf := new(bytes.Buffer)

	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(msg); err != nil {
		return nil, errors.Wrap(err, "cannot encode message")
	}

	return buf.Bytes(), nil
}

func (GobMarshaler) Unmarshal(stanMsg *stan.Msg) (*message.Message, error) {
	// todo - use pool
	buf := new(bytes.Buffer)

	_, err := buf.Write(stanMsg.Data)
	if err != nil {
		return nil, errors.Wrap(err, "cannot write stan message data to buffer")
	}

	decoder := gob.NewDecoder(buf)

	var decodedMsg message.Message
	if err := decoder.Decode(&decodedMsg); err != nil {
		return nil, errors.Wrap(err, "cannot decode message")
	}

	// creating clean message, to avoid invalid internal state with ack
	msg := message.NewMessage(decodedMsg.UUID, decodedMsg.Payload)
	msg.Metadata = decodedMsg.Metadata

	return msg, nil
}
