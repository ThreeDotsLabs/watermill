package message

type Payload interface{}

type Message interface {
	UUID() string // todo - change to []byte?, change to type

	GetMetadata(key string) string
	AllMetadata() map[string]string
}

type ProducedMessage interface {
	Message

	SetMetadata(key, value string)
	Payload() Payload
}

type ConsumedMessage interface {
	Message

	UnmarshalPayload(val interface{}) error

	Acknowledged() (<-chan error)
	Acknowledge() error
	Error(err error) error
}

type Base struct {
	MessageUUID     string
	MessageMetadata map[string]string

	*Ack
}

func (m Base) UUID() string {
	return m.MessageUUID
}

func (m *Base) SetMetadata(key, value string) {
	m.MessageMetadata[key] = value
}

func (m *Base) GetMetadata(key string) string {
	if val, ok := m.MessageMetadata[key]; ok {
		return val
	}

	return ""
}

func (m Base) AllMetadata() map[string]string {
	return m.MessageMetadata
}

// defaultImpl is default Message implementation.
// todo - rename?
type defaultMessage struct {
	Base

	payload Payload
}

func NewDefault(uuid string, payload Payload) ProducedMessage {
	return &defaultMessage{
		Base{
			MessageUUID:     uuid,
			MessageMetadata: make(map[string]string),

			Ack: NewAck(),
		},
		payload,
	}
}

func (m *defaultMessage) Payload() Payload {
	return m.payload
}
