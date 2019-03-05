package sql

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

// SQLAdapter executes the appropriate SQL queries in the chosen dialect and according to its specific schema.
type SQLAdapter interface {
	// InsertMessages inserts rows representing messages on a topic.
	// Order of messages must be preserved so that SelectMessage reads them in the order they were published.
	InsertMessages(ctx context.Context, topic string, messages ...*message.Message) error

	// PopMessage returns subsquent undelivered messages on topic in the order they were published.
	// Offsets should be tracked separately for separate consumer groups.
	// Empty string is allowed as consumer group.
	//
	// Once ACKed, the same message should not be delivered more than once to a consumer group, but it is is not
	// guaranteed in this simple implementation.
	// The receiving party should deduplicate or make sure that the message's effects are idempotent.
	// todo: implement only once
	PopMessage(ctx context.Context, topic string, consumerGroup string) (*message.Message, error)
}
