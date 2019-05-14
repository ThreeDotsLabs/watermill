package sql_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	driver "github.com/go-sql-driver/mysql"

	std_sql "database/sql"
)

//
//func TestMain(m *testing.M) {
//	db, err := connectMySQL()
//	if err != nil {
//		fmt.Println(err.Error())
//		os.Exit(1)
//	}
//
//	setupErr := setup(db)
//	if setupErr != nil {
//		fmt.Println(setupErr.Error())
//		if teardownErr := teardown(db); teardownErr != nil {
//			fmt.Println(teardownErr.Error())
//		}
//		os.Exit(1)
//	}
//
//	exitCode := m.Run()
//	teardownErr := teardown(db)
//	if teardownErr != nil {
//		fmt.Println(teardownErr.Error())
//	}
//
//	os.Exit(exitCode)
//}
//
//func setup(db *std_sql.DB) (err error) {
//	var tx *std_sql.Tx
//	tx, err = db.Begin()
//	if err != nil {
//		return errors.Wrap(err, "could not begin tx")
//	}
//	defer func() {
//		if err != nil {
//			rollbackErr := tx.Rollback()
//			if rollbackErr != nil {
//				fmt.Println(rollbackErr.Error())
//			}
//		} else {
//			commitErr := tx.Commit()
//			if commitErr != nil {
//				fmt.Println(commitErr.Error())
//			}
//		}
//	}()
//
//	_, err = tx.Exec(`
//		CREATE TABLE IF NOT EXISTS messages_test (
//		  offset BIGINT AUTO_INCREMENT,
//		  uuid VARCHAR(255) NOT NULL,
//		  payload VARBINARY(255) NULL,
//		  metadata JSON NULL,
//		  topic VARCHAR(255) NOT NULL,
//		  PRIMARY KEY (uuid),
//		  UNIQUE(offset)
//		);`)
//	if err != nil {
//		return errors.Wrap(err, "could not create messages table")
//	}
//
//	_, err = tx.Exec(`
//		CREATE TABLE IF NOT EXISTS offsets_acked_test (
//		  offset BIGINT NOT NULL,
//		  consumer_group VARCHAR(255) NOT NULL,
//		  PRIMARY KEY(consumer_group),
//		  FOREIGN KEY (offset) REFERENCES messages_test(offset)
//		);`)
//	if err != nil {
//		return errors.Wrap(err, "could not create offsets table")
//	}
//
//	return nil
//}
//
//func teardown(db *std_sql.DB) (err error) {
//	var tx *std_sql.Tx
//	tx, err = db.Begin()
//	if err != nil {
//		return errors.Wrap(err, "could not begin tx")
//	}
//	defer func() {
//		if err != nil {
//			rollbackErr := tx.Rollback()
//			if rollbackErr != nil {
//				fmt.Println(rollbackErr.Error())
//			}
//		} else {
//			commitErr := tx.Commit()
//			if commitErr != nil {
//				fmt.Println(commitErr.Error())
//			}
//		}
//	}()
//
//	//_, err = tx.Exec(`DROP TABLE IF EXISTS offsets_acked_test;`)
//	//if err != nil {
//	//	return errors.Wrap(err, "could not drop message offsets table")
//	//}
//	//
//	//_, err = tx.Exec(`DROP TABLE IF EXISTS messages_test;`)
//	//if err != nil {
//	//	return errors.Wrap(err, "could not drop messages table")
//	//}
//
//	return nil
//}

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
