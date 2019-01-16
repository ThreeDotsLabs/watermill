package command

import "github.com/ThreeDotsLabs/watermill/message"

type Marshaler interface {
	Marshal(cmd interface{}) (*message.Message, error)
	Unmarshal(msg *message.Message, cmd interface{}) (err error)
	CommandName(cmd interface{}) string
	MarshaledCommandName(msg *message.Message) string
}
