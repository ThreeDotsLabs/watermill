package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/delay"

	"github.com/jmoiron/sqlx"
)

type PostgresMessage struct {
	Offset   int    `db:"offset"`
	UUID     string `db:"uuid"`
	Payload  string `db:"payload"`
	Metadata string `db:"metadata"`
}

type PostgresRepository struct {
	db *sqlx.DB
}

func NewPostgresRepository(dbURL string) (*PostgresRepository, error) {
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		return nil, err
	}

	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) AllMessages(topic string) ([]Message, error) {
	var dbMessages []PostgresMessage
	// TODO custom table name?
	err := r.db.Select(&dbMessages, fmt.Sprintf(`SELECT "offset", uuid, payload, metadata FROM watermill_%v`, topic))
	if err != nil {
		return nil, err
	}

	var messages []Message

	for _, dbMsg := range dbMessages {
		var metadata map[string]string
		err := json.Unmarshal([]byte(dbMsg.Metadata), &metadata)
		if err != nil {
			return nil, err
		}

		msg, err := NewMessage(fmt.Sprint(dbMsg.Offset), dbMsg.UUID, dbMsg.Payload, metadata)
		if err != nil {
			return nil, err
		}

		messages = append(messages, msg)
	}

	return messages, nil
}

func (r *PostgresRepository) Requeue(topic string, id string) error {
	_, err := r.db.Exec(fmt.Sprintf(`UPDATE watermill_%v SET metadata = metadata::jsonb || jsonb_build_object($1::text, $2::text) WHERE "offset" = $3`, topic),
		delay.DelayedUntilKey, time.Now().UTC().Format(time.RFC3339), id,
	)
	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresRepository) Ack(topic string, id string) error {
	_, err := r.db.Exec(fmt.Sprintf(`UPDATE watermill_%v SET acked = true WHERE "offset" = %v`, topic, id))
	if err != nil {
		return err
	}

	return nil
}
