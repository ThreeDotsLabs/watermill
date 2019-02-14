package io

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

type UnmarshalMessageFunc func(topic string, b []byte) (*message.Message, error)
type MarshalMessageFunc func(topic string, msg *message.Message) ([]byte, error)

var (
	hashFunc = sha1.Sum
)

// PayloadUnmarshalFunc puts the whole byte slice into the message's Payload.
// The UUID is generated from the byte slice by the SHA1 hash function.
func PayloadUnmarshalFunc(topic string, b []byte) (*message.Message, error) {
	uuid := fmt.Sprintf("%x", hashFunc(b))
	return message.NewMessage(uuid, b), nil
}

// PayloadMarshalFunc dumps the message's payload, discarding the remaining fields of the message.
// The output is always terminated with EOL byte.
//
// This basic unmarshaler function may be used e.g. to write just the message payloads to stdout or to a file,
// without cluttering the output with metadata and UUIDs.
func PayloadMarshalFunc(topic string, msg *message.Message) ([]byte, error) {
	return append(msg.Payload, '\n'), nil
}

// PrettyPayloadMarshalFunc dumps the message's payload.
// Each message is prepended by the current timestamp and the topic.
// The output is always terminated with EOL byte.
//
// This basic unmarshaler function may be used e.g. to write just the message payloads to stdout or to a file,
// without cluttering the output with metadata and UUIDs.
func PrettyPayloadMarshalFunc(topic string, msg *message.Message) ([]byte, error) {
	buf := &bytes.Buffer{}

	_, err := fmt.Fprintf(buf, "[%s] %s: %s\n", time.Now().Format("2006-01-02 15:04:05.999999999"), topic, string(msg.Payload))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
