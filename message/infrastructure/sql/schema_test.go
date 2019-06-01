package sql_test

import (
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// testSchema implements Inserter, Selecter and Acker for PubSubTests. It works with the schema:
//
// `offset` BIGINT(20) NOT NULL AUTO_INCREMENT,
// `uuid` VARCHAR(255) NOT NULL,
// `payload` VARBINARY(255) DEFAULT NULL,
// `metadata` JSON DEFAULT NULL,
// `topic` VARCHAR(255) NOT NULL,
//
// testSchema maintains a separate table for each topic, which helps to prevent deadlock in parallel tests.
type testSchema struct {
	insertQ string
	selectQ string
	ackQ    string

	// db is needed to create a separate table per topic if needed
	db *sql.DB
}

func (s testSchema) messagesTable(topic string) string {
	return "`test_" + topic + "`"
}

func (s testSchema) messagesOffsetsTable(topic string) string {
	return "`test_ack_" + topic + "`"
}

func (s *testSchema) EnsureTableForTopicQueries(topic string) []string {
	createMessagesTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.messagesTable(topic) + " (",
		"`offset` BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY,",
		"`uuid` VARCHAR(255) NOT NULL,",
		"`payload` VARBINARY(255) DEFAULT NULL,",
		"`metadata` JSON DEFAULT NULL,",
		"`topic` VARCHAR(255) NOT NULL",
		");",
	}, "\n")
	createAcksTable := strings.Join([]string{
		"CREATE TABLE IF NOT EXISTS " + s.messagesOffsetsTable(topic) + " (",
		"`offset` BIGINT NOT NULL,",
		"`consumer_group` VARCHAR(255) NOT NULL,",
		"PRIMARY KEY(consumer_group),",
		"FOREIGN KEY (offset) REFERENCES " + s.messagesTable(topic) + "(offset)",
		");",
	}, "\n")

	return []string{createMessagesTable, createAcksTable}
}

func (s *testSchema) InsertQuery(topic string) string {
	insertQ := strings.Join([]string{
		`INSERT INTO`,
		s.messagesTable(topic),
		`(uuid, payload, metadata, topic) VALUES (?,?,?,?)`,
	}, " ")

	logger.Info("Preparing query to insert messages", watermill.LogFields{
		"q": insertQ,
	})

	s.insertQ = insertQ
	return insertQ
}

func (s *testSchema) InsertArgs(topic string, msg *message.Message) (args []interface{}, err error) {
	if s.insertQ != "" {
		logger = logger.With(watermill.LogFields{
			"q": s.insertQ,
		})
	}
	defer func() {
		if err != nil {
			logger.Error("Could not marshal message into SQL insert args", err, nil)
			return
		}
		logger.Debug("Marshaled message into insert args", watermill.LogFields{
			"uuid": msg.UUID,
		})
	}()

	if len(msg.UUID) > 255 {
		return nil, errors.New("the message UUID does not fit into VARCHAR(255)")
	}

	var metadata []byte
	metadata, err = json.Marshal(msg.Metadata)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal metadata into JSON")
	}

	return []interface{}{
		msg.UUID,
		msg.Payload,
		metadata,
		topic,
	}, nil
}

func (s *testSchema) AckQuery(topic string) string {
	ackQ := strings.Join([]string{
		`INSERT INTO `, s.messagesOffsetsTable(topic), ` (offset, consumer_group) `,
		`VALUES (?, ?) ON DUPLICATE KEY UPDATE offset=VALUES(offset)`,
	}, "")

	logger.Info("Preparing query to ack messages", watermill.LogFields{
		"q": ackQ,
	})

	s.ackQ = ackQ
	return ackQ
}

func (s *testSchema) AckArgs(offset int, consumerGroup string) ([]interface{}, error) {
	return []interface{}{offset, consumerGroup}, nil
}

func (s *testSchema) SelectQuery(topic string) string {
	selectQ := strings.Join([]string{
		`SELECT offset,uuid,payload,metadata FROM `, s.messagesTable(topic),
		` WHERE TOPIC=? AND `, s.messagesTable(topic), `.offset >`,
		` (SELECT COALESCE(MAX(`, s.messagesOffsetsTable(topic), `.offset), 0) FROM `, s.messagesOffsetsTable(topic),
		` WHERE consumer_group=?)`,
		` ORDER BY `, s.messagesTable(topic), `.offset ASC LIMIT 1`,
	}, "")

	logger.Info("Preparing query to select messages", watermill.LogFields{
		"q": selectQ,
	})

	s.ackQ = selectQ
	return selectQ
}

func (s *testSchema) SelectArgs(topic string, consumerGroup string) ([]interface{}, error) {
	if len(topic) > 255 {
		return nil, errors.New("the topic does not fit into VARCHAR(255)")
	}
	return []interface{}{topic, consumerGroup}, nil
}

func (s *testSchema) UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	var (
		uuid     string
		payload  []byte
		metadata []byte
	)
	err = row.Scan(&offset, &uuid, &payload, &metadata)
	if err != nil {
		return 0, nil, errors.Wrap(err, "could not scan message row")
	}

	msg = message.NewMessage(uuid, payload)

	if metadata != nil {
		err = json.Unmarshal(metadata, &msg.Metadata)
		if err != nil {
			return 0, nil, errors.Wrap(err, "could not unmarshal metadata as JSON")
		}
	}

	return offset, msg, nil

}
