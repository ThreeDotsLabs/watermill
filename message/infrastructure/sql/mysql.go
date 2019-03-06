package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

type MySQLDefaultAdapterConf struct {
	// MessagesTable is the name of the table to read messages from. Defaults to `messages`.
	MessagesTable string
	// OffsetsAckedTable stores the information about which consumer group has acked which message. Defaults to `offsets_acked`.
	OffsetsAckedTable string

	Logger watermill.LoggerAdapter
}

func (c *MySQLDefaultAdapterConf) setDefaults() {
	if c.MessagesTable == "" {
		c.MessagesTable = "messages"
	}
	if c.OffsetsAckedTable == "" {
		c.OffsetsAckedTable = "offsets_acked"
	}
	if c.Logger == nil {
		c.Logger = watermill.NopLogger{}
	}
}

type mysqlDefaultAdapterRow struct {
	Offset    int64
	UUID      []byte
	CreatedAt time.Time
	Payload   []byte
	Topic     string
	Metadata  []byte
}

// MySQLDefaultAdapter is an adapter for MySQL.
// Is compatible with the following schema:
// uuid BINARY(16),
// payload JSON,
// metadata JSON,
// topic VARCHAR(255)
type MySQLDefaultAdapter struct {
	conf MySQLDefaultAdapterConf
	db   *sql.DB

	insertQ    string
	insertStmt *sql.Stmt

	selectQ    string
	selectStmt *sql.Stmt

	insertOffsetQ    string
	insertOffsetStmt *sql.Stmt

	// messageOffsets saves the offset retrieved for database for each message that GetMessage returns.
	// when MarkAcked is called with the message, we know what offset to save.
	messageOffsetsLock *sync.Mutex
	messageOffsets     map[*message.Message]int64
}

func NewMySQLDefaultAdapter(db *sql.DB, conf MySQLDefaultAdapterConf) (*MySQLDefaultAdapter, error) {
	conf.setDefaults()
	if db == nil {
		return nil, errors.New("db is nil")
	}

	insertQ := fmt.Sprintf(`INSERT INTO %s (uuid, payload, topic, metadata) VALUES (?, ?, ?, ?)`, conf.MessagesTable)
	insertStmt, err := db.Prepare(insertQ)
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare insert statement")
	}

	// todo: this is hardly readable, maybe use text/template? complicated tho
	selectQ := fmt.Sprintf(
		`SELECT offset,uuid,payload,topic,metadata FROM %s `+
			`WHERE TOPIC=? `+
			`AND %s.offset > (SELECT COALESCE(MAX(%s.offset), 0) FROM %s WHERE consumer_group=?) `+
			`ORDER BY %s.offset ASC LIMIT 1`,
		conf.MessagesTable,
		conf.MessagesTable,
		conf.OffsetsAckedTable,
		conf.OffsetsAckedTable,
		conf.MessagesTable,
	)
	selectStmt, err := db.Prepare(selectQ)
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare select statement")
	}

	insertOffsetQ := fmt.Sprintf(
		`INSERT INTO %s (offset, consumer_group) VALUES (?, ?) ON DUPLICATE KEY UPDATE offset=VALUES(offset)`,
		conf.OffsetsAckedTable,
	)
	insertOffsetStmt, err := db.Prepare(insertOffsetQ)
	if err != nil {
		return nil, errors.Wrap(err, "could not prepare insert offset statement")
	}

	return &MySQLDefaultAdapter{
		conf,
		db,

		insertQ,
		insertStmt,

		selectQ,
		selectStmt,

		insertOffsetQ,
		insertOffsetStmt,

		&sync.Mutex{},
		make(map[*message.Message]int64),
	}, nil
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

	tx, err := a.db.BeginTx(ctx, nil)
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

	stmt := tx.Stmt(a.insertStmt)
	for _, msg := range messages {
		id, metadata, argsErr := a.insertArgs(msg)
		if argsErr != nil {
			err = multierror.Append(err, errors.Wrap(argsErr, "could not prepare args for insert"))
			continue
		}

		a.conf.Logger.Trace("Inserting message", watermill.LogFields{
			"q":     a.insertQ,
			"uuid":  msg.UUID,
			"topic": topic,
		})

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

func (a *MySQLDefaultAdapter) GetMessage(ctx context.Context, topic string, consumerGroup string) (*message.Message, error) {
	tx, err := a.db.BeginTx(ctx, nil)
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
		return nil, errors.Wrap(err, "could not begin transaction")
	}

	logger := a.conf.Logger.With(watermill.LogFields{
		"topic":          topic,
		"consumer_group": consumerGroup,
	})

	stmt := tx.Stmt(a.selectStmt)

	row := stmt.QueryRowContext(ctx, topic, consumerGroup)
	msg, offset, err := a.unmarshal(row)
	if err != nil {
		return nil, err
	}

	logger.Trace("Message found", watermill.LogFields{
		"q": a.selectQ,
	})

	a.messageOffsetsLock.Lock()
	a.messageOffsets[msg] = offset
	a.messageOffsetsLock.Unlock()

	return msg, nil
}

func (a *MySQLDefaultAdapter) unmarshal(row *sql.Row) (*message.Message, int64, error) {
	dest := mysqlDefaultAdapterRow{}
	err := row.Scan(&dest.Offset, &dest.UUID, &dest.Payload, &dest.Topic, &dest.Metadata)
	if err != nil {
		return nil, 0, errors.Wrap(err, "could not scan row")
	}

	if len(dest.UUID) != 16 {
		return nil, 0, errors.New("uuid length not suitable for unmarshaling to ULID")
	}

	uuid := ulid.ULID{}
	for i := 0; i < 16; i++ {
		uuid[i] = dest.UUID[i]
	}

	msg := message.NewMessage(uuid.String(), dest.Payload)

	if dest.Metadata != nil {
		err = json.Unmarshal(dest.Metadata, &msg.Metadata)
		if err != nil {
			return nil, 0, errors.Wrap(err, "could not unmarshal metadata as JSON")
		}
	}

	return msg, dest.Offset, nil
}

func (a *MySQLDefaultAdapter) MarkAcked(ctx context.Context, msg *message.Message, consumerGroup string) (err error) {
	var tx *sql.Tx
	tx, err = a.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "could not begin transaction")
	}

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

	a.conf.Logger.Trace("Updating consumer group offset", watermill.LogFields{
		"uuid":           msg.UUID,
		"consumer_group": consumerGroup,
		"q":              a.insertOffsetQ,
	})

	offset, err := a.getOffset(msg)
	if err != nil {
		return err
	}

	stmt := tx.Stmt(a.insertOffsetStmt)
	_, err = stmt.ExecContext(
		ctx,
		offset,
		consumerGroup,
	)

	return err
}

func (a *MySQLDefaultAdapter) getOffset(msg *message.Message) (int64, error) {
	a.messageOffsetsLock.Lock()
	defer a.messageOffsetsLock.Unlock()
	offset, ok := a.messageOffsets[msg]

	if !ok {
		return 0, errors.New("unknown offset for message")
	}

	delete(a.messageOffsets, msg)

	return offset, nil
}
