package mysql

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// InsertArgs is used as arguments for the SQL INSERT statements used in the Publisher.
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
