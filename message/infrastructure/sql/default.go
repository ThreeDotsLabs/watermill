package sql

import (
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// DefaultSchema is a default implementation of Inserter, Selecter and Acker that works with the following schema:
//
// `offset` bigint(20) NOT NULL AUTO_INCREMENT,
// `uuid` binary(16) NOT NULL,
// `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
// `payload` json DEFAULT NULL,
// `metadata` json DEFAULT NULL,
// `topic` varchar(255) NOT NULL,
type DefaultSchema struct {
	Logger  watermill.LoggerAdapter
	insertQ string
	selectQ string
	ackQ    string
}

func (s *DefaultSchema) InsertQuery(messagesTable string) string {
	if s.Logger == nil {
		s.Logger = watermill.NopLogger{}
	}

	insertQ := strings.Join([]string{
		`INSERT INTO`,
		messagesTable,
		`(uuid, payload, metadata, topic) VALUES (?,?,?,?)`,
	}, " ")

	s.Logger.Info("Preparing query to insert messages", watermill.LogFields{
		"q": insertQ,
	})

	s.insertQ = insertQ
	return insertQ
}

func (s *DefaultSchema) InsertArgs(topic string, msg *message.Message) (args []interface{}, err error) {
	logger := s.Logger
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
			"uuid":  msg.UUID,
			"topic": topic,
		})
	}()

	if len(topic) > 255 {
		return nil, errors.New("the topic does not fit into VARCHAR(255)")
	}

	var uuid ulid.ULID
	uuid, err = ulid.Parse(msg.UUID)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse message UUID as ULID")
	}

	uuidBytes := make([]byte, 16)
	err = uuid.MarshalBinaryTo(uuidBytes)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal UUID to ULID bytes")
	}

	var metadata []byte
	metadata, err = json.Marshal(msg.Metadata)
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal metadata into JSON")
	}

	return []interface{}{
		uuidBytes,
		msg.Payload,
		metadata,
		topic,
	}, nil
}

func (s *DefaultSchema) AckQuery(messageOffsetsTable string, consumerGroup string) string {
	if s.Logger == nil {
		s.Logger = watermill.NopLogger{}
	}

	ackQ := strings.Join([]string{
		`INSERT INTO`, messageOffsetsTable, `(offset, consumer_group) `,
		`VALUES (?, "`, consumerGroup, `") ON DUPLICATE KEY UPDATE offset=VALUES(offset)`,
	}, "")

	s.Logger.Info("Preparing query to ack messages", watermill.LogFields{
		"q": ackQ,
	})

	s.ackQ = ackQ
	return ackQ
}

func (s *DefaultSchema) AckArgs(offset int) ([]interface{}, error) {
	return []interface{}{offset}, nil
}

func (s *DefaultSchema) SelectQuery(messagesTable string, messagesAckedTable string, consumerGroup string) string {
	if s.Logger == nil {
		s.Logger = watermill.NopLogger{}
	}

	selectQ := strings.Join([]string{
		`SELECT offset,uuid,payload,metadata FROM `, messagesTable,
		`WHERE TOPIC=? AND `, messagesTable, `.offset > `,
		`(SELECT COALESCE(MAX(`, messagesAckedTable, `.offset), 0) FROM `, messagesAckedTable,
		`WHERE consumer_group=`, consumerGroup,
		` ORDER BY {{.MessagesTable}}.offset ASC LIMIT 1`,
	}, "")

	s.Logger.Info("Preparing query to select messages", watermill.LogFields{
		"q": selectQ,
	})

	s.ackQ = selectQ
	return selectQ
}

type defaultSchemaRow struct {
	Offset    int64
	UUID      []byte
	CreatedAt time.Time
	Payload   []byte
	Metadata  []byte
}

func (s *DefaultSchema) UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	r := defaultSchemaRow{}
	err = row.Scan(&r.Offset, &r.UUID, &r.CreatedAt, &r.Payload, &r.Metadata)
	if err != nil {
		return 0, nil, errors.Wrap(err, "could not scan message row")
	}
	if len(r.UUID) != 16 {
		return 0, nil, errors.New("uuid length not suitable for unmarshaling to ULID")
	}

	uuid := ulid.ULID{}
	for i := 0; i < 16; i++ {
		uuid[i] = r.UUID[i]
	}

	msg = message.NewMessage(uuid.String(), r.Payload)

	if r.Metadata != nil {
		err = json.Unmarshal(r.Metadata, &msg.Metadata)
		if err != nil {
			return 0, nil, errors.Wrap(err, "could not unmarshal metadata as JSON")
		}
	}

	return int(r.Offset), msg, nil

}
