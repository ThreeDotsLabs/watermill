package io

import (
	"bytes"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

type MarshalMessageFunc func(topic string, b []byte) (*message.Message, error)
type UnmarshalMessageFunc func(topic string, msg *message.Message) ([]byte, error)

// PayloadUnmarshalFunc dumps the message's payload, discarding the remaining fields of the message.
// The output is always terminated with EOL byte.
//
// This basic unmarshaler function may be used e.g. to write just the message payloads to stdout or to a file,
// without cluttering the output with metadata and UUIDs.
func PayloadUnmarshalFunc(topic string, msg *message.Message) ([]byte, error) {
	return append(msg.Payload, '\n'), nil
}

// PrettyPayloadUnmarshalFunc dumps the message's payload.
// Each message is prepended by the current timestamp and the topic.
// The output is always terminated with EOL byte.
//
// This basic unmarshaler function may be used e.g. to write just the message payloads to stdout or to a file,
// without cluttering the output with metadata and UUIDs.
func PrettyPayloadUnmarshalFunc(topic string, msg *message.Message) ([]byte, error) {
	buf := &bytes.Buffer{}

	_, err := fmt.Fprintf(buf, "[%s] %s: %s\n", time.Now().Format("2006-01-02 15:04:05.999999999"), topic, string(msg.Payload))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
