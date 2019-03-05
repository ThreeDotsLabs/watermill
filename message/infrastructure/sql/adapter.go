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
	// SelectMessage returns the messages on topic in the order they were published.
	// Different consumer groups should track their own offsets.
	//
	// The same message should not be delivered more than once to each consumer group, but it is is not
	// guaranteed at this point, so the receiving party should deduplicate or make sure that the effects are idempotent.
	// todo: implement only once?
	SelectMessage(ctx context.Context, topic string, consumerGroup string) (*message.Message, error)
}
