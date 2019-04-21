package sql

// Acker provides methods for storing the information about which messages have been acked by which consumer group.
// Acker is schema-dependent.
type Acker interface {
	// AckQuery returns the SQL query that will mark a message as read for a given consumer group.
	// Subscriber will not return those messages again for this consumer group.
	AckQuery(messageOffsetsTable string) string
	// AckArgs transforms the recovered message's offset and consumer group into the arguments put into AckQuery.
	AckArgs(offset int, consumerGroup string) ([]interface{}, error)
}
