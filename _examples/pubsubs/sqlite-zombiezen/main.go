// Sources for https://watermill.io/docs/getting-started/
package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sqlite/wmsqlitezombiezen"
	"github.com/ThreeDotsLabs/watermill/message"
	_ "modernc.org/sqlite"
	"zombiezen.com/go/sqlite"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	// &cache=shared is critical, see: https://github.com/zombiezen/go-sqlite/issues/92#issuecomment-2052330643
	connectionDSN := "file:db.sqlite3?journal_mode=WAL&busy_timeout=1000&secure_delete=true&foreign_keys=true&cache=shared"

	conn, err := sqlite.OpenConn(connectionDSN)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	publisher, err := wmsqlitezombiezen.NewPublisher(conn, wmsqlitezombiezen.PublisherOptions{
		InitializeSchema: true,
		Logger:           logger,
	})
	if err != nil {
		panic(err)
	}

	subscriber, err := wmsqlitezombiezen.NewSubscriber(connectionDSN, wmsqlitezombiezen.SubscriberOptions{
		InitializeSchema: true,
		Logger:           logger,
	})
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), "example_topic")
	if err != nil {
		panic(err)
	}

	go process(messages)
	publishMessages(publisher)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte(`{"message": "Hello from ZombieZen!"}`))

		if err := publisher.Publish("example_topic", msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("ZombieZen received message: %s, payload: %s", msg.UUID, string(msg.Payload))
		msg.Ack()
	}
}
