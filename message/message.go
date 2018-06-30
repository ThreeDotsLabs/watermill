package message

import (
	"github.com/mitchellh/mapstructure"
)

type Payload interface{}

type Message interface {
	UUID() string // todo - change to []byte?, change to type

	SetMetadata(key, value string)
	GetMetadata(key string) string
	AllMetadata() map[string]string

	UnmarshalPayload(val interface{}) error

	Acknowledged() (<-chan error)
	Acknowledge() error
	Error(err error) error
}

// Default is default Message implementation.
type Default struct {
	MessageUUID     string            `json:"message_uuid"`
	MessageMetadata map[string]string `json:"message_metadata"`
	MessagePayload  Payload           `json:"message_payload"`

	*Ack
}

func NewDefault(uuid string, payload Payload) Message {
	return &Default{
		MessageUUID:     uuid,
		MessageMetadata: make(map[string]string),
		MessagePayload:  payload,

		Ack: NewAck(),
	}
}

func NewEmptyDefault() Message {
	return &Default{
		MessageMetadata: make(map[string]string),
		Ack:             NewAck(),
	}
}

func (m Default) UUID() string {
	return m.MessageUUID
}

func (m *Default) SetMetadata(key, value string) {
	m.MessageMetadata[key] = value
}

func (m *Default) GetMetadata(key string) string {
	if val, ok := m.MessageMetadata[key]; ok {
		return val
	}

	return ""
}

func (m Default) AllMetadata() map[string]string {
	return m.MessageMetadata
}

func (m *Default) UnmarshalPayload(val interface{}) error {
	// todo - detect immutable
	return mapstructure.Decode(m.MessagePayload, val)
}
