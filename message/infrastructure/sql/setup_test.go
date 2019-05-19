package sql_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	driver "github.com/go-sql-driver/mysql"

	std_sql "database/sql"
)

func connectMySQL() (*std_sql.DB, error) {
	conf := driver.Config{
		User:                 "root",
		Passwd:               "",
		Net:                  "",
		Addr:                 "localhost",
		DBName:               "watermill",
		AllowNativePasswords: true,
	}
	db, err := std_sql.Open("mysql", conf.FormatDSN())
	if err != nil {
		return nil, errors.Wrap(err, "could not open mysql connection")
	}

	err = db.Ping()
	if err != nil {
		return nil, errors.Wrap(err, "could not ping mysql connection")
	}

	return db, nil
}

func newMySQL(t *testing.T) *std_sql.DB {
	db, err := connectMySQL()
	require.NoError(t, err)
	return db
}
