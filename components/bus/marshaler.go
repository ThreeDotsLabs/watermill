package bus

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

type MessageMarshaler interface {
	Marshal(ctx context.Context, v any) (*message.Message, error)
	Unmarshal(msg *message.Message, v any) (err error)
	Type(v any) string
	TypeFromMessage(msg *message.Message) string
}
