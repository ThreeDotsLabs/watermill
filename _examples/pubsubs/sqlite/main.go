// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sqlite/wmsqlitemodernc"
	"github.com/ThreeDotsLabs/watermill/message"
	_ "modernc.org/sqlite"
)

func main() {
	db := createDB()
	defer db.Close()
	logger := watermill.NewStdLogger(false, false)

	subscriber, err := wmsqlitemodernc.NewSubscriber(
		db,
		wmsqlitemodernc.SubscriberOptions{
			InitializeSchema: true,
			Logger:           logger,
		},
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), "example_topic")
	if err != nil {
		panic(err)
	}

	go process(messages)

	publisher, err := wmsqlitemodernc.NewPublisher(
		db,
		wmsqlitemodernc.PublisherOptions{
			InitializeSchema: true,
			Logger:           logger,
		},
	)
	if err != nil {
		panic(err)
	}
	publishMessages(publisher)
}

func createDB() *sql.DB {
	connectionDSN := ":memory:" // or "db.sqlite3?journal_mode=WAL&busy_timeout=1000&cache=shared"
	db, err := sql.Open("sqlite", connectionDSN)
	if err != nil {
		panic(err)
	}
	// limit the number of concurrent connections to one
	// this is a limitation of `modernc.org/sqlite` driver
	db.SetMaxOpenConns(1)

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	return db
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte(`{"message": "Hello, world!"}`))

		if err := publisher.Publish("example_topic", msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
