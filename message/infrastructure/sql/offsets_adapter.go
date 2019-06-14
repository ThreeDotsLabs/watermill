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

type DefaultOffsetsAdapter struct {
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (s DefaultOffsetsAdapter) SchemaInitializingQueries(topic string) []string {
	return []string{`
		CREATE TABLE IF NOT EXISTS ` + s.MessagesOffsetsTable(topic) + ` (
		consumer_group VARCHAR(255) NOT NULL,
		offset BIGINT NOT NULL,
		PRIMARY KEY(consumer_group)
	)`}
}

func (s DefaultOffsetsAdapter) AckMessageQuery(topic string, offset int, consumerGroup string) (string, []interface{}) {
	ackQuery := `INSERT INTO ` + s.MessagesOffsetsTable(topic) + ` (offset, consumer_group) 
		VALUES (?, ?) ON DUPLICATE KEY UPDATE offset=VALUES(offset)`

	return ackQuery, []interface{}{offset, consumerGroup}
}

func (s DefaultOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) (string, []interface{}) {
	return `
		SELECT COALESCE(MAX(` + s.MessagesOffsetsTable(topic) + `.offset), 0)
		FROM ` + s.MessagesOffsetsTable(topic) + `
		WHERE consumer_group=? FOR UPDATE`, []interface{}{consumerGroup}
	// todo - test for update for update
}

func (s DefaultOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if s.GenerateMessagesOffsetsTableName != nil {
		return s.GenerateMessagesOffsetsTableName(topic)
	}
	return "watermill_offsets_" + topic
}

func (s DefaultOffsetsAdapter) ConsumedMessageQuery(
	topic string,
	offset int,
	consumerGroup string,
	consumerULID []byte,
) (string, []interface{}) {
	// this offsets adapter relay only on acked messages
	return "", nil
}
