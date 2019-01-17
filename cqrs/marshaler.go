package cqrs

import "github.com/ThreeDotsLabs/watermill/message"

// todo - rename?
type Marshaler interface {
	Marshal(v interface{}) (*message.Message, error)
	Unmarshal(msg *message.Message, v interface{}) (err error)
	Name(cmdOrEvent interface{}) string        // todo - rename?
	MarshaledName(msg *message.Message) string // todo - rename?
}
