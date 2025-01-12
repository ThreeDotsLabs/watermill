package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
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

func (m mysqlSchemaAdapter) SchemaInitializingQueries(params sql.SchemaInitializingQueriesParams) ([]sql.Query, error) {
	createQuery := `
		CREATE TABLE IF NOT EXISTS ` + params.Topic + ` (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			user VARCHAR(36) NOT NULL,
			first_name VARCHAR(36) NOT NULL,
			last_name VARCHAR(36) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`

	return []sql.Query{{Query: createQuery}}, nil
}

func (m mysqlSchemaAdapter) InsertQuery(params sql.InsertQueryParams) (sql.Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (user, first_name, last_name, created_at) VALUES %s`,
		params.Topic,
		strings.TrimRight(strings.Repeat(`(?,?,?,?),`, len(params.Msgs)), ","),
	)

	var args []interface{}
	for _, msg := range params.Msgs {
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

func (m mysqlSchemaAdapter) SelectQuery(params sql.SelectQueryParams) (sql.Query, error) {
	nextOffsetQuery, err := params.OffsetsAdapter.NextOffsetQuery(sql.NextOffsetQueryParams{
		Topic:         params.Topic,
		ConsumerGroup: params.ConsumerGroup,
	})
	if err != nil {
		return sql.Query{}, err
	}

	selectQuery := `
		SELECT id, user, first_name, last_name, created_at FROM ` + params.Topic + `
		WHERE
			id > (` + nextOffsetQuery.Query + `)
		ORDER BY
			id ASC
		LIMIT 1`

	return sql.Query{Query: selectQuery, Args: nextOffsetQuery.Args}, nil
}

func (m mysqlSchemaAdapter) UnmarshalMessage(params sql.UnmarshalMessageParams) (_ sql.Row, err error) {
	user := mysqlUser{}
	err = params.Row.Scan(&user.ID, &user.User, &user.FirstName, &user.LastName, &user.CreatedAt)
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
