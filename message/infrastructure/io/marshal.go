package io

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
)

// MarshalMessageFunc packages the message into a byte slice.
// The topic argument is there because some writers (i.e. loggers) might want to present the topic as part of their output.
type MarshalMessageFunc func(topic string, msg *message.Message) ([]byte, error)

// PayloadMarshalFunc dumps the message's payload, discarding the remaining fields of the message.
// The output is always terminated with EOL byte.
//
// This basic unmarshaler function may be used e.g. to write just the message payloads to stdout or to a file,
// without cluttering the output with metadata and UUIDs.
func PayloadMarshalFunc(topic string, msg *message.Message) ([]byte, error) {
	return append(msg.Payload, '\n'), nil
}

// TimestampTopicPayloadMarshalFunc dumps the message's payload.
// Each message is prepended by the current timestamp and the topic.
// The output is always terminated with EOL byte.
//
// This basic unmarshaler function may be used e.g. to write just the message payloads to stdout or to a file,
// without cluttering the output with metadata and UUIDs.
func TimestampTopicPayloadMarshalFunc(topic string, msg *message.Message) ([]byte, error) {
	buf := &bytes.Buffer{}

	_, err := fmt.Fprintf(
		buf,
		"[%s] %s: %s\n",
		time.Now().Format("2006-01-02 15:04:05.999999999"),
		topic,
		string(msg.Payload),
	)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// UnmarshalMessageFunc restores the message from a byte slice.
// The topic argument is there to keep symmetry with MarshalMessageFunc, as some unmarshalers might restore the topic as well.
type UnmarshalMessageFunc func(topic string, b []byte) (*message.Message, error)

// PayloadUnmarshalFunc puts the whole byte slice into the message's Payload.
// The UUID is generated from the byte slice by the SHA1 hash function.
func PayloadUnmarshalFunc(topic string, b []byte) (*message.Message, error) {
	uuid := watermill.NewUUID()
	return message.NewMessage(uuid, b), nil
}

// LosslessMarshaler marshals/unmarshals messages using gob.
// As opposed to other (un)marshalers in this package, all the attributes of the message (UUID, metadata, ...) are preserved.
// However, the result is not easily readable by humans or other marshalers.
type LosslessMarshaler struct{}

func (m LosslessMarshaler) Unmarshal(topic string, b []byte) (*message.Message, error) {
	buf := new(bytes.Buffer)
	decoder := gob.NewDecoder(buf)

	_, err := buf.Write(b)
	if err != nil {
		return nil, errors.Wrap(err, "could not write on gob buffer")
	}

	var msg = new(message.Message)
	err = decoder.Decode(msg)
	if err != nil {
		return nil, errors.Wrap(err, "could not decode message with gob")
	}

	return msg, nil
}

func (m LosslessMarshaler) Marshal(topic string, msg *message.Message) ([]byte, error) {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)

	err := encoder.Encode(msg)

	if err != nil {
		return nil, errors.Wrap(err, "could not encode message with gob")
	}

	b := buf.Bytes()
	return b, nil
}
