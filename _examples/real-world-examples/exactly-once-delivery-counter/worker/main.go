package main

import (
	"context"
	stdSQL "database/sql"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"github.com/ThreeDotsLabs/watermill/message"
	driver "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
)

const topic = "counter"

func main() {
	db := createDB()
	logger := watermill.NewStdLogger(false, false)

	go runWatermillRouter(db, logger)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// no graceful shutdown, to increase chance of problems :-)
	<-sigs
}

type messagePayload struct {
	CounterUUID string `json:"counter_uuid"`
}

func runWatermillRouter(db *stdSQL.DB, logger watermill.LoggerAdapter) {
	subscriber, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			SchemaAdapter:    sql.DefaultMySQLSchema{},
			OffsetsAdapter:   sql.DefaultMySQLOffsetsAdapter{},
			InitializeSchema: true,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddNoPublisherHandler(
		"counter",
		topic,
		subscriber,
		processMessage,
	)

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}

func processMessage(msg *message.Message) error {
	tx, ok := sql.TxFromContext(msg.Context())
	if !ok {
		return errors.New("tx not found in message context")
	}

	payload := messagePayload{}
	err := json.Unmarshal(msg.Payload, &payload)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal payload")
	}

	// let's do it more fragile, let's get the value from DB instead of simple increment
	counterValue, err := dbCounterValue(msg.Context(), tx, payload.CounterUUID)
	if err != nil {
		return err
	}

	counterValue += 1

	if err := updateDbCounter(msg.Context(), tx, payload.CounterUUID, counterValue); err != nil {
		return err
	}

	return nil
}

func updateDbCounter(ctx context.Context, tx *stdSQL.Tx, counterUUD string, counterValue int) error {
	_, err := tx.ExecContext(
		ctx,
		"INSERT INTO counter (id, value) VALUES (?, ?) ON DUPLICATE KEY UPDATE value = ?",
		counterUUD,
		counterValue,
		counterValue,
	)
	if err != nil {
		return errors.Wrap(err, "can't update counter value")
	}

	return nil
}

func dbCounterValue(ctx context.Context, tx *stdSQL.Tx, counterUUID string) (int, error) {
	var counterValue int
	row := tx.QueryRowContext(ctx, "SELECT value from counter WHERE id = ?", counterUUID)

	if err := row.Scan(&counterValue); err != nil {
		switch err {
		case stdSQL.ErrNoRows:
			return 0, nil
		default:
			return 0, errors.Wrap(err, "can't get counter value")
		}
	}

	return counterValue, nil
}

func createDB() *stdSQL.DB {
	conf := driver.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = "mysql"
	conf.DBName = "example"

	db, err := stdSQL.Open("mysql", conf.FormatDSN())
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	return db
}
