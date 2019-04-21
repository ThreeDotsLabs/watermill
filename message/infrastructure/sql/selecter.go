package sql

import (
	"database/sql"

	"github.com/ThreeDotsLabs/watermill/message"
)

// Selecter provides methods for retrieving the messages that are schema-dependent.
type Selecter interface {
	// SelectQuery returns the SQL query that returns the next unread message for a given consumer group.
	// Subscriber will not return those messages again for this consumer group.
	SelectQuery(messagesTable string, messagesAckedTable string, consumerGroup string) string
	// SelectArgs transforms the topic into the argument put into SelectQuery.
	SelectArgs(topic string) ([]interface{}, error)
	// UnmarshalMessage transforms the Row obtained from the SQL query into a Watermill message.
	// It also returns the offset of the last read message, for the purpose of acking.
	UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error)
}
