package main

import (
	"context"
	"database/sql"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sqlite/wmsqlitemodernc"
	"github.com/ThreeDotsLabs/watermill/message"
)

func publishWithInTransaction(db *sql.DB) {
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	publisher, err := wmsqlitemodernc.NewPublisher(
		tx,
		wmsqlitemodernc.PublisherOptions{
			// schema must be initialized elsewhere before using
			// the publisher within the transaction
			InitializeSchema: false,
		},
	)
	if err != nil {
		panic(err)
	}

	msg := message.NewMessage(watermill.NewUUID(), []byte(`{"message": "Hello, world!"}`))
	if err := publisher.Publish("example_topic", msg); err != nil {
		_ = tx.Rollback()
		panic(err)
	}
}
