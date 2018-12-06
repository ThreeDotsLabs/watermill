package nats

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/go-nats-streaming"
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

var gobBuffers = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type GobMarshaler struct{}

func (GobMarshaler) Marshal(topic string, msg *message.Message) ([]byte, error) {
	buf := gobBuffers.Get().(*bytes.Buffer)
	defer gobBuffers.Put(buf)

	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(msg); err != nil {
		return nil, errors.Wrap(err, "cannot encode message")
	}

	return buf.Bytes(), nil
}

func (GobMarshaler) Unmarshal(stanMsg *stan.Msg) (*message.Message, error) {
	buf := gobBuffers.Get().(*bytes.Buffer)
	defer gobBuffers.Put(buf)
	buf.Reset()

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
