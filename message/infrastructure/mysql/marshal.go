package mysql

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// InsertArgs are used as arguments for the SQL INSERT statements used in the Publisher.
type InsertArgs struct {
	UUID     []byte
	Payload  []byte
	Metadata []byte
	Topic    string
}

// Marshaler transforms the Watermill message into arguments for an SQL INSERT.
//
// The data types in InsertArgs are intentionally broad, but in the SQL schema they might be more specific.
// It is the responsibility of the Marshaler to throw an error if the message may not be transformed
// into the targeted schema, for example due to size limits of a column.
type Marshaler interface {
	ForInsert(topic string, msg *message.Message) (InsertArgs, error)
}

// DefaultMarshaler is compatible with the following schema:
// uuid BINARY(16),
// payload JSON,
// metadata JSON,
// topic VARCHAR(255)
type DefaultMarshaler struct{}

// ForInsert of DefaultMarshaler makes the following assumptions:
// - uuid may be parsed to ULID.
// - payload contains valid JSON
// - topic is at most 255 characters long.
//
// Error will be thrown if these assumtions are not met.
func (DefaultMarshaler) ForInsert(topic string, msg *message.Message) (InsertArgs, error) {
	id, err := ulid.Parse(msg.UUID)
	if err != nil {
		return InsertArgs{}, errors.Wrap(err, "could not parse message id as ULID")
	}

	var idBytes = make([]byte, 16)
	if err = id.MarshalBinaryTo(idBytes); err != nil {
		return InsertArgs{}, errors.Wrap(err, "could not represent message id as BINARY(16)")
	}

	if len(idBytes) > 16 {
		return InsertArgs{}, errors.New("could not fit message in into BINARY(16)")
	}

	metadata, err := json.Marshal(msg.Metadata)
	if err != nil {
		return InsertArgs{}, errors.Wrap(err, "could not marshal message metadata to JSON")
	}

	if len(topic) > 255 {
		return InsertArgs{}, errors.New("topic does not fit into VARCHAR(255)")
	}

	return InsertArgs{
		idBytes,
		msg.Payload,
		metadata,
		topic,
	}, nil
}

type MessageWithOffset struct {
	Offset int64
	*message.Message
}

type Unmarshaler interface {
	// SelectColumns returns the slice of column names that should be passed to the SELECT query that recovers the events.
	// The Row that Unmarshal takes will have these columns available for Scan.
	SelectColumns() []string
	// Unmarshal takes an sql Row result and returns the message that it contains and its offset.
	Unmarshal(row *sql.Row) (MessageWithOffset, error)
}

// DefaultUnmarshaler is compatible with the following schema:
// uuid BINARY(16),
// topic VARCHAR(255)
type DefaultUnmarshaler struct{}

type dbTransport struct {
	Offset    int64
	UUID      []byte
	CreatedAt time.Time
	Payload   []byte
	Topic     string
	Metadata  []byte
}

func (DefaultUnmarshaler) SelectColumns() []string {
	return []string{
		"offset", "uuid", "payload", "topic", "metadata",
	}
}

func (DefaultUnmarshaler) Unmarshal(row *sql.Row) (MessageWithOffset, error) {
	t := dbTransport{}
	err := row.Scan(&t.Offset, &t.UUID, &t.Payload, &t.Topic, &t.Metadata)
	if err != nil {
		return MessageWithOffset{}, errors.Wrap(err, "could not scan row")
	}

	if len(t.UUID) != 16 {
		return MessageWithOffset{}, errors.New("uuid length not suitable for unmarshaling to ULID")
	}

	uuid := ulid.ULID{}
	for i := 0; i < 16; i++ {
		uuid[i] = t.UUID[i]
	}

	metadata := message.Metadata{}
	err = json.Unmarshal(t.Metadata, &metadata)
	if err != nil {
		return MessageWithOffset{}, errors.Wrap(err, "could not unmarshal metadata as JSON")
	}

	msg := message.NewMessage(uuid.String(), t.Payload)
	msg.Metadata = metadata

	return MessageWithOffset{Offset: t.Offset, Message: msg}, nil
}
