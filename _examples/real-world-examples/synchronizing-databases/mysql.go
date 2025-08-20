package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
)

type mysqlUser struct {
	ID        int64
	User      string
	FirstName string
	LastName  string
	CreatedAt time.Time
}

type mysqlSchemaAdapter struct {
	sql.DefaultMySQLSchema
}

func (m mysqlSchemaAdapter) SchemaInitializingQueries(topic string) []sql.Query {
	createQuery := `
		CREATE TABLE IF NOT EXISTS ` + topic + ` (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			user VARCHAR(36) NOT NULL,
			first_name VARCHAR(36) NOT NULL,
			last_name VARCHAR(36) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`

	return []sql.Query{{Query: createQuery}}
}

func (m mysqlSchemaAdapter) InsertQuery(topic string, msgs message.Messages) (sql.Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (user, first_name, last_name, created_at) VALUES %s`,
		topic,
		strings.TrimRight(strings.Repeat(`(?,?,?,?),`, len(msgs)), ","),
	)

	var args []interface{}
	for _, msg := range msgs {
		user := mysqlUser{}

		decoder := gob.NewDecoder(bytes.NewBuffer(msg.Payload))
		err := decoder.Decode(&user)
		if err != nil {
			return sql.Query{}, err
		}

		args = append(args, user.User, user.FirstName, user.LastName, user.CreatedAt)
	}

	return sql.Query{Query: insertQuery, Args: args}, nil
}

func (m mysqlSchemaAdapter) SelectQuery(topic string, consumerGroup string, offsetsAdapter sql.OffsetsAdapter) sql.Query {
	nextOffsetQuery := offsetsAdapter.NextOffsetQuery(topic, consumerGroup)
	selectQuery := `
		SELECT id, user, first_name, last_name, created_at FROM ` + topic + `
		WHERE
			id > (` + nextOffsetQuery.Query + `)
		ORDER BY
			id ASC
		LIMIT 1`

	return sql.Query{Query: selectQuery, Args: nextOffsetQuery.Args}
}

func (m mysqlSchemaAdapter) UnmarshalMessage(row sql.Scanner) (_ sql.Row, err error) {
	user := mysqlUser{}
	err = row.Scan(&user.ID, &user.User, &user.FirstName, &user.LastName, &user.CreatedAt)
	if err != nil {
		return sql.Row{}, err
	}

	var payload bytes.Buffer
	encoder := gob.NewEncoder(&payload)

	err = encoder.Encode(user)
	if err != nil {
		return sql.Row{}, err
	}

	msg := message.NewMessage(watermill.NewULID(), payload.Bytes())

	return sql.Row{
		Offset: user.ID,
		Msg:    msg,
	}, nil
}
