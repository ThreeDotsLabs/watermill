package sql

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// Inserter provides methods for inserting the published messages that are schema-dependent.
type Inserter interface {
	// InsertQuery returns the SQL query that will insert the Watermill message into the SQL storage.
	InsertQuery(topic string) string
	// InsertArgs transforms the topic and Watermill message into the arguments put into InsertQuery.
	InsertArgs(topic string, msg *message.Message) ([]interface{}, error)
}
