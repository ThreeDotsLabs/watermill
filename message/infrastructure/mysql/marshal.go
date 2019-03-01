package mysql

import (
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

// SelectArgs are used as arguments for the SQL SELECT statements used in the Subscriber.
type SelectArgs struct {
	Idx   int64
	Topic string
}

type Unmarshaler interface {
	// ForSelect provides the parameters for an SQL SELECT statement looking for messages from given topic and offset.
	// The data types in SelectArgs are intentionally broad, but in the SQL schema they might be more specific.
	// It is the responsibility of the Unmarshaler to throw an error if the parameters may not be transformed
	// into the targeted schema, for example due to size limits of a column.
	ForSelect(index int64, topic string) (SelectArgs, error)
	// Unmarshal takes an sql Rows result and returns the messages that it contains.
	Unmarshal(transport dbTransport) (*message.Message, error)
}

// DefaultUnmarshaler is compatible with the following schema:
// uuid BINARY(16),
// topic VARCHAR(255)
type DefaultUnmarshaler struct{}

// ForSelect of DefaultUnmarshaler makes the following assumptions:
// - uuid may be parsed to ULID.
// - topic is at most 255 characters long.
//
// Error will be thrown if these assumtions are not met.
func (DefaultUnmarshaler) ForSelect(index int64, topic string) (SelectArgs, error) {
	if len(topic) > 255 {
		return SelectArgs{}, errors.New("topic does not fit into VARCHAR(255)")
	}

	return SelectArgs{index, topic}, nil
}

type dbTransport struct {
	Idx       int64
	UUID      []byte
	CreatedAt time.Time
	Payload   []byte
	Topic     string
	Metadata  []byte
}

func (DefaultUnmarshaler) Unmarshal(transport dbTransport) (*message.Message, error) {
	if len(transport.UUID) != 16 {
		return nil, errors.New("uuid length not suitable for unmarshaling to ULID")
	}

	uuid := ulid.ULID{}
	for i := 0; i < 16; i++ {
		uuid[i] = transport.UUID[i]
	}

	metadata := message.Metadata{}
	err := json.Unmarshal(transport.Metadata, &metadata)
	if err != nil {
		return nil, errors.Wrap(err, "could not unmarshal metadata as JSON")
	}

	msg := message.NewMessage(uuid.String(), transport.Payload)
	msg.Metadata = metadata

	return msg, nil
}
