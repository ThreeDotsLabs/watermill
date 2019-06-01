package sql_test

import (
	"strings"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/sql"
)

// testSchema makes the following changes to DefaultSchema to comply with tests:
// - uuid is a VARCHAR(255) instead of VARCHAR(32); some UUIDs in tests are bigger and we don't care for storage use
// - payload is a VARBINARY(255) instead of JSON; tests don't presuppose JSON-marshallable payloads
type testSchema struct {
	sql.DefaultSchema
}

func (s *testSchema) SchemaInitializingQueries(topic string) []string {
	createMessagesTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesTable(topic) + " (",
		"`offset` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,",
		"`uuid` VARCHAR(255) NOT NULL,",
		"`payload` VARBINARY(255) DEFAULT NULL,",
		"`metadata` JSON DEFAULT NULL",
		");",
	}, "\n")
	createAcksTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.MessagesOffsetsTable(topic) + " (",
		"`offset` BIGINT NOT NULL,",
		"`consumer_group` VARCHAR(255) NOT NULL,",
		"PRIMARY KEY(consumer_group),",
		"FOREIGN KEY (offset) REFERENCES " + s.MessagesTable(topic) + "(offset)",
		");",
	}, "\n")

	return []string{createMessagesTable, createAcksTable}
}
