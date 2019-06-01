package sql

import (
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// SchemaAdapter produces the SQL queries and arguments appropriately for a specific schema and dialect
// It also transforms sql.Rows into Watermill messages.
type SchemaAdapter interface {
	// AckQuery returns the SQL query that will mark a message as read for a given consumer group.
	// Subscriber will not return those messages again for this consumer group.
	AckQuery(topic string) string
	// AckArgs transforms the recovered message's offset and consumer group into the arguments put into AckQuery.
	AckArgs(offset int, consumerGroup string) ([]interface{}, error)

	// InsertQuery returns the SQL query that will insert the Watermill message into the SQL storage.
	InsertQuery(topic string) string
	// InsertArgs transforms the topic and Watermill message into the arguments put into InsertQuery.
	InsertArgs(topic string, msg *message.Message) ([]interface{}, error)

	// SelectQuery returns the SQL query that returns the next unread message for a given consumer group.
	// Subscriber will not return those messages again for this consumer group.
	SelectQuery(topic string) string
	// SelectArgs transforms the topic into the argument put into SelectQuery.
	SelectArgs(topic string, consumerGroup string) ([]interface{}, error)
	// UnmarshalMessage transforms the Row obtained from the SQL query into a Watermill message.
	// It also returns the offset of the last read message, for the purpose of acking.
	UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error)

	// SchemaInitializingQueries returns SQL queries which will make sure (CREATE IF NOT EXISTS)
	// that the appropriate tables exist to write messages to the given topic.
	SchemaInitializingQueries(topic string) []string
}

// DefaultSchema is a default implementation of SchemaAdapter that works with the following schema:
//
// `offset` BIGINT NOT NULL AUTO_INCREMENT,
// `uuid` VARCHAR(32) NOT NULL,
// `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
// `payload` JSON DEFAULT NULL,
// `metadata` JSON DEFAULT NULL,
type DefaultSchema struct {
	// GenerateMessagesTableName may be used to override how the messages table name is generated.
	GenerateMessagesTableName func(topic string) string
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (s *DefaultSchema) SchemaInitializingQueries(topic string) []string {
	createMessagesTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesTable(topic) + " (",
		"`offset` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,",
		"`uuid` VARCHAR(32) NOT NULL,",
		"`created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,",
		"`payload` JSON DEFAULT NULL,",
		"`metadata` JSON DEFAULT NULL",
		");",
	}, "\n")
	createAcksTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesOffsetsTable(topic) + " (",
		"`offset` BIGINT NOT NULL,",
		"`consumer_group` VARCHAR(255) NOT NULL,",
		"PRIMARY KEY(consumer_group),",
		"FOREIGN KEY (offset) REFERENCES " + s.MessagesTable(topic) + "(offset)",
		")",
	}, "\n")

	return []string{createMessagesTable, createAcksTable}
}

func (s *DefaultSchema) InsertQuery(topic string) string {
	insertQuery := strings.Join([]string{
		`INSERT INTO`,
		s.MessagesTable(topic),
		`(uuid, payload, metadata) VALUES (?,?,?)`,
	}, " ")

	return insertQuery
}

func (s *DefaultSchema) InsertArgs(topic string, msg *message.Message) (args []interface{}, err error) {
	var metadata []byte
	metadata, err = json.Marshal(msg.Metadata)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal metadata into JSON")
	}

	return []interface{}{
		msg.UUID,
		msg.Payload,
		metadata,
	}, nil
}

func (s *DefaultSchema) AckQuery(topic string) string {
	ackQuery := strings.Join([]string{
		`INSERT INTO `, s.MessagesOffsetsTable(topic), ` (offset, consumer_group) `,
		`VALUES (?, ?) ON DUPLICATE KEY UPDATE offset=VALUES(offset)`,
	}, "")

	return ackQuery
}

func (s *DefaultSchema) AckArgs(offset int, consumerGroup string) ([]interface{}, error) {
	return []interface{}{offset, consumerGroup}, nil
}

func (s *DefaultSchema) SelectQuery(topic string) string {
	selectQuery := strings.Join([]string{
		`SELECT offset,uuid,payload,metadata FROM `, s.MessagesTable(topic),
		` WHERE `, s.MessagesTable(topic), `.offset >`,
		` (SELECT COALESCE(MAX(`, s.MessagesOffsetsTable(topic), `.offset), 0) FROM `, s.MessagesOffsetsTable(topic),
		` WHERE consumer_group=?)`,
		` ORDER BY `, s.MessagesTable(topic), `.offset ASC LIMIT 1`,
	}, "")

	return selectQuery
}

func (s *DefaultSchema) SelectArgs(topic string, consumerGroup string) ([]interface{}, error) {
	return []interface{}{consumerGroup}, nil
}

type defaultSchemaRow struct {
	Offset   int64
	UUID     []byte
	Payload  []byte
	Metadata []byte
}

func (s *DefaultSchema) UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	r := defaultSchemaRow{}
	err = row.Scan(&r.Offset, &r.UUID, &r.Payload, &r.Metadata)
	if err != nil {
		return 0, nil, errors.Wrap(err, "could not scan message row")
	}

	msg = message.NewMessage(string(r.UUID), r.Payload)

	if r.Metadata != nil {
		err = json.Unmarshal(r.Metadata, &msg.Metadata)
		if err != nil {
			return 0, nil, errors.Wrap(err, "could not unmarshal metadata as JSON")
		}
	}

	return int(r.Offset), msg, nil
}

func (s DefaultSchema) MessagesTable(topic string) string {
	if s.GenerateMessagesTableName != nil {
		return "`" + s.GenerateMessagesTableName(topic) + "`"
	}
	return "`watermill_" + topic + "`"
}

func (s DefaultSchema) MessagesOffsetsTable(topic string) string {
	if s.GenerateMessagesOffsetsTableName != nil {
		return "`" + s.GenerateMessagesOffsetsTableName(topic) + "`"
	}
	return "`watermill_acked" + topic + "`"
}
