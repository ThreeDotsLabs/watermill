package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

// MySQLDefaultAdapter is an adapter for MySQL.
// Is compatible with the following schema:
// uuid BINARY(16),
// payload JSON,
// metadata JSON,
// topic VARCHAR(255)
type MySQLDefaultAdapter struct {
	DB *sql.DB
	// MessagesTable is the name of the table to read messages from. Defaults to `messages`.
	MessagesTable string
}

// InsertMessages of DefaultMarshaler makes the following assumptions about the message:
// - uuid may be parsed to ULID.
// - payload contains valid JSON.
// - topic is at most 255 characters long.
//
// Error will be thrown if these assumtions are not met.
func (a *MySQLDefaultAdapter) InsertMessages(ctx context.Context, topic string, messages ...*message.Message) (err error) {
	if len(topic) > 255 {
		return errors.New("topic does not fit into VARCHAR(255)")
	}

	tx, err := a.DB.BeginTx(ctx, nil)
	defer func() {
		if err != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				err = multierror.Append(err, rollbackErr)
			}
		} else {
			commitErr := tx.Commit()
			if commitErr != nil {
				err = multierror.Append(err, commitErr)
			}
		}

	}()
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}

	stmt, err := tx.Prepare(
		fmt.Sprintf(`INSERT INTO %s (uuid, payload, topic, metadata) VALUES (?, ?, ?, ?)`, a.MessagesTable),
	)
	if err != nil {
		return errors.Wrap(err, "could not prepare statement")
	}
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil {
			err = multierror.Append(err, closeErr)
		}
	}()

	for _, msg := range messages {
		id, metadata, argsErr := a.insertArgs(msg)
		if argsErr != nil {
			err = multierror.Append(err, errors.Wrap(argsErr, "could not prepare args for insert"))
			continue
		}

		_, execErr := stmt.ExecContext(ctx, id, msg.Payload, topic, metadata)
		if execErr != nil {
			err = multierror.Append(err, errors.Wrap(execErr, "could not execute stmt"))
		}
	}

	return err
}

func (a MySQLDefaultAdapter) insertArgs(msg *message.Message) (idBytes []byte, metadata []byte, err error) {
	id, err := ulid.Parse(msg.UUID)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not parse message id as ULID")
	}

	idBytes = make([]byte, 16)
	if err = id.MarshalBinaryTo(idBytes); err != nil {
		return nil, nil, errors.Wrap(err, "could not represent message id as BINARY(16)")
	}

	if len(idBytes) > 16 {
		return nil, nil, errors.New("could not fit message in into BINARY(16)")
	}

	metadata, err = json.Marshal(msg.Metadata)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not marshal message metadata to JSON")
	}

	return idBytes, metadata, nil
}

func (a *MySQLDefaultAdapter) SelectMessage(ctx context.Context, topic string, consumerGroup string) (*message.Message, error) {
	panic("implement me")
}
