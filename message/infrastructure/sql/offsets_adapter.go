package sql

type OffsetsAdapter interface {
	// AckQuery returns the SQL query that will mark a message as read for a given consumer group.
	// Subscriber will not return those messages again for this consumer group.
	AckMessageQuery(topic string, offset int, consumerGroup string) (string, []interface{})

	// ConsumedMessageQuery will be called after consuming message, but before ack.
	// ConsumedMessageQuery is optional, and will be not executed if query is empty.
	ConsumedMessageQuery(topic string, offset int, consumerGroup string, consumerULID []byte) (string, []interface{})

	// NextOffsetQuery returns new message offset for provided consumer group.
	NextOffsetQuery(topic, consumerGroup string) (string, []interface{})

	// SchemaInitializingQueries returns SQL queries which will make sure (CREATE IF NOT EXISTS)
	// that the appropriate tables exist to write messages to the given topic.
	SchemaInitializingQueries(topic string) []string
}

// todo - more docs how it's working
type DefaultMySQLOffsetsAdapter struct {
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (s DefaultMySQLOffsetsAdapter) SchemaInitializingQueries(topic string) []string {
	return []string{`
		CREATE TABLE IF NOT EXISTS ` + s.MessagesOffsetsTable(topic) + ` (
		consumer_group VARCHAR(255) NOT NULL,
		offset_acked BIGINT,
		offset_consumed BIGINT NOT NULL,
		PRIMARY KEY(consumer_group)
	)`}
}

func (s DefaultMySQLOffsetsAdapter) AckMessageQuery(topic string, offset int, consumerGroup string) (string, []interface{}) {
	ackQuery := `UPDATE ` + s.MessagesOffsetsTable(topic) + ` SET offset_acked=? WHERE consumer_group = ?`

	return ackQuery, []interface{}{offset, consumerGroup}
}

func (s DefaultMySQLOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) (string, []interface{}) {
	// We are using FOR UPDATE to lock consumer group in offsets table.
	// Tested in in TestConcurrentSubscribe.
	return `
		SELECT COALESCE(MAX(offset_acked), 0)
		FROM ` + s.MessagesOffsetsTable(topic) + `
		WHERE consumer_group=? FOR UPDATE`, []interface{}{consumerGroup}
}

func (s DefaultMySQLOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if s.GenerateMessagesOffsetsTableName != nil {
		return s.GenerateMessagesOffsetsTableName(topic)
	}
	return "watermill_offsets_" + topic
}

func (s DefaultMySQLOffsetsAdapter) ConsumedMessageQuery(
	topic string,
	offset int,
	consumerGroup string,
	consumerULID []byte,
) (string, []interface{}) {
	ackQuery := `INSERT INTO ` + s.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group) 
		VALUES (?, ?) ON DUPLICATE KEY UPDATE offset_consumed=VALUES(offset_consumed)`

	return ackQuery, []interface{}{offset, consumerGroup}
}
