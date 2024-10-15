package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/tools/pq/cli"
)

type PostgresMessage struct {
	Offset   int    `db:"offset"`
	UUID     string `db:"uuid"`
	Payload  string `db:"payload"`
	Metadata string `db:"metadata"`
}

type PostgresBackend struct {
	db     *sqlx.DB
	config cli.BackendConfig
}

func NewPostgresBackend(ctx context.Context, config cli.BackendConfig) (*PostgresBackend, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, fmt.Errorf("missing DATABASE_URL")
	}

	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		return nil, err
	}

	return &PostgresBackend{
		db:     db,
		config: config,
	}, nil
}

func (r *PostgresBackend) AllMessages(ctx context.Context) ([]cli.Message, error) {
	var dbMessages []PostgresMessage
	// TODO custom table name?
	err := r.db.SelectContext(ctx, &dbMessages, fmt.Sprintf(`SELECT "offset", uuid, payload, metadata FROM %v WHERE acked = false`, r.topic()))
	if err != nil {
		return nil, err
	}

	var messages []cli.Message

	for _, dbMsg := range dbMessages {
		var metadata map[string]string
		err := json.Unmarshal([]byte(dbMsg.Metadata), &metadata)
		if err != nil {
			return nil, err
		}

		msg, err := cli.NewMessage(fmt.Sprint(dbMsg.Offset), dbMsg.UUID, dbMsg.Payload, metadata)
		if err != nil {
			return nil, err
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

func (r *PostgresBackend) Requeue(ctx context.Context, msg cli.Message) error {
	_, err := r.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %v SET metadata = metadata::jsonb || jsonb_build_object($1::text, $2::text) WHERE "offset" = $3`, r.topic()),
		delay.DelayedUntilKey, time.Now().UTC().Format(time.RFC3339), msg.ID,
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresBackend) Ack(ctx context.Context, msg cli.Message) error {
	_, err := r.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %v SET acked = true WHERE "offset" = %v`, r.topic(), msg.ID))
	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresBackend) topic() string {
	if r.config.Topic != "" {
		return fmt.Sprintf(`"watermill_%v"`, r.config.Topic)
	}

	return fmt.Sprintf(`"%v"`, r.config.RawTopic)
}
