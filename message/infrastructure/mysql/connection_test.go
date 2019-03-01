package mysql_test

import (
	"database/sql"
	"testing"

	driver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func getDB(t *testing.T) *sql.DB {
	conf := driver.Config{
		User:                 "root",
		Passwd:               "",
		Net:                  "",
		Addr:                 "localhost",
		DBName:               "watermill",
		AllowNativePasswords: true,
	}
	db, err := sql.Open("mysql", conf.FormatDSN())
	require.NoError(t, err)

	return db
}

func TestGetDB(t *testing.T) {
	db := getDB(t)
	require.NoError(t, db.Ping())
}
