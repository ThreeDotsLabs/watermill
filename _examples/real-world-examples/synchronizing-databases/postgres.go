package main

import (
	"bytes"
	stdSQL "database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

type postgresUser struct {
	ID        int
	Username  string
	FullName  string
	CreatedAt time.Time
}

type postgresSchemaAdapter struct{}

func (p postgresSchemaAdapter) SchemaInitializingQueries(topic string) []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS ` + topic + ` (
			id INT NOT NULL PRIMARY KEY,
			username VARCHAR(36) NOT NULL,
			full_name VARCHAR(36) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);`,
	}
}

func (p postgresSchemaAdapter) InsertQuery(topic string, msgs message.Messages) (string, []interface{}, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (id, username, full_name, created_at) VALUES %s`,
		topic,
		strings.TrimRight(strings.Repeat(`($1,$2,$3,$4),`, len(msgs)), ","),
	)

	var args []interface{}
	for _, msg := range msgs {
		user := postgresUser{}

		decoder := gob.NewDecoder(bytes.NewBuffer(msg.Payload))
		err := decoder.Decode(&user)
		if err != nil {
			return "", nil, err
		}

		args = append(args, user.ID, user.Username, user.FullName, user.CreatedAt)
	}

	return insertQuery, args, nil
}

func (p postgresSchemaAdapter) SelectQuery(topic string, consumerGroup string, offsetsAdapter sql.OffsetsAdapter) (string, []interface{}) {
	// No need to implement this method, as PostgreSQL subscriber is not used in this example.
	return "", nil
}

func (p postgresSchemaAdapter) UnmarshalMessage(row *stdSQL.Row) (offset int, msg *message.Message, err error) {
	return 0, nil, errors.New("not implemented")
}
