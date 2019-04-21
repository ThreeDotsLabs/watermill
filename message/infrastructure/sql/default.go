package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"

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
}

var defaultInsertQueryTemplate = `INSERT INTO %s (uuid, payload, metadata, topic) VALUES (?,?,?,?)`
var defaultSelectQueryTemplate = ``
var defaultAckQueryTemplate = ``

func (s *DefaultSchema) InsertQuery(messagesTable string) string {
	if s.Logger == nil {
		s.Logger = watermill.NopLogger{}
	}
	insertQ := fmt.Sprintf(defaultInsertQueryTemplate, messagesTable)
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

func (s *DefaultSchema) AckQuery(messageOffsetsTable string) string {
	panic("implement me")
}

func (s *DefaultSchema) AckArgs(offset int, consumerGroup string) ([]interface{}, error) {
	panic("implement me")
}

func (s *DefaultSchema) SelectQuery(messagesTable string, messagesAckedTable string, consumerGroup string) string {
	panic("implement me")
}

func (s *DefaultSchema) UnmarshalMessage(row *sql.Row) (offset int, msg *message.Message, err error) {
	panic("implement me")
}
