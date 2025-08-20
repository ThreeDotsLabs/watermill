package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
)

type postgresUser struct {
	ID        int64
	Username  string
	FullName  string
	CreatedAt time.Time
}

type postgresSchemaAdapter struct {
	sql.DefaultPostgreSQLSchema
}

func (p postgresSchemaAdapter) SchemaInitializingQueries(params sql.SchemaInitializingQueriesParams) ([]sql.Query, error) {
	createQuery := `
		CREATE TABLE IF NOT EXISTS ` + params.Topic + ` (
			id INT NOT NULL PRIMARY KEY,
			username VARCHAR(36) NOT NULL,
			full_name VARCHAR(36) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
	`

	return []sql.Query{{Query: createQuery}}, nil
}

func (p postgresSchemaAdapter) InsertQuery(params sql.InsertQueryParams) (sql.Query, error) {
	insertQuery := fmt.Sprintf(
		`INSERT INTO %s (id, username, full_name, created_at) VALUES %s`,
		params.Topic,
		strings.TrimRight(strings.Repeat(`($1,$2,$3,$4),`, len(params.Msgs)), ","),
	)

	var args []interface{}
	for _, msg := range params.Msgs {
		user := postgresUser{}

		decoder := gob.NewDecoder(bytes.NewBuffer(msg.Payload))
		err := decoder.Decode(&user)
		if err != nil {
			return sql.Query{}, err
		}

		args = append(args, user.ID, user.Username, user.FullName, user.CreatedAt)
	}

	return sql.Query{Query: insertQuery, Args: args}, nil
}

func (p postgresSchemaAdapter) SelectQuery(params sql.SelectQueryParams) (sql.Query, error) {
	// No need to implement this method, as PostgreSQL subscriber is not used in this example.
	return sql.Query{}, nil
}

func (p postgresSchemaAdapter) UnmarshalMessage(params sql.UnmarshalMessageParams) (sql.Row, error) {
	return sql.Row{}, errors.New("not implemented")
}
