package main

import (
	"encoding/json"
	"fmt"

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

func (r *PostgresRepository) Requeue(offset int) error {
	//TODO implement me
	panic("implement me")
}

func (r *PostgresRepository) Delete(offset int) error {
	//TODO implement me
	panic("implement me")
}
