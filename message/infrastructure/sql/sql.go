package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// db is implemented both by *sql.DB and *sql.Tx
type db interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// sqlArgsToLog is used for "lazy" generating sql args strings to logger
type sqlArgsToLog []interface{}

func (s sqlArgsToLog) String() string {
	var strArgs []string
	for _, arg := range s {
		strArgs = append(strArgs, fmt.Sprintf("%s", arg))
	}

	return strings.Join(strArgs, ",")
}

func isDeadlock(err error) bool {
	// ugly, but should be universal for multiple sql implementations
	return strings.Contains(strings.ToLower(err.Error()), "deadlock")
}
