package mysql

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

type ConnectionConfig struct {
	Addr     string
	Database string
	Table    string

	User     string
	Password string
}

func (c ConnectionConfig) validate() error {
	if c.Addr == "" {
		return errors.New("addr not set")
	}

	if c.Database == "" {
		return errors.New("database not set")
	}

	if c.Table == "" {
		return errors.New("table not set")
	}

	return nil
}

// connect returns a new DB connection for the given credentials.
// The caller is responsible for closing the connection when done.
func (c *ConnectionConfig) connect() (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@%s?%s", c.User, c.Password, c.Addr, c.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Wrap(err, "could not open mysql connection")
	}

	if err = db.Ping(); err != nil {
		return nil, errors.Wrap(err, "could not check mysql connection")
	}

	return db, nil
}
