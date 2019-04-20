package sql

import (
	"encoding/json"
	"fmt"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Inserter provides methods for inserting the published messages that are schema-dependent.
type Inserter interface {
	InsertQuery(messagesTable string) string
	InsertArgs(topic string, msg *message.Message) ([]interface{}, error)
}

// DefaultInserter is the inserter that works with the following schema:
//
// `offset` bigint(20) NOT NULL AUTO_INCREMENT,
// `uuid` binary(16) NOT NULL,
// `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
// `payload` json DEFAULT NULL,
// `metadata` json DEFAULT NULL,
// `topic` varchar(255) NOT NULL,
type DefaultInserter struct {
	Logger  watermill.LoggerAdapter
	insertQ string
}

var defaultInserterQueryTemplate = `INSERT INTO %s (uuid, payload, metadata, topic) VALUES (?,?,?,?)`

func (i *DefaultInserter) InsertQuery(messagesTable string) string {
	if i.Logger == nil {
		i.Logger = watermill.NopLogger{}
	}
	insertQ := fmt.Sprintf(defaultInserterQueryTemplate, messagesTable)
	i.Logger.Info("Preparing query to insert messages", watermill.LogFields{
		"q": insertQ,
	})

	i.insertQ = insertQ
	return insertQ
}

// InsertArgs assumes that:
// - the payload may be marshaled to valid JSON
// - the topic is no longer than 255 characters
// - UUID may be parsed into ULID.
func (i DefaultInserter) InsertArgs(topic string, msg *message.Message) (args []interface{}, err error) {
	logger := i.Logger
	if i.insertQ != "" {
		logger = logger.With(watermill.LogFields{
			"q": i.insertQ,
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
