package main

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sqlite/wmsqlitezombiezen"
	"github.com/ThreeDotsLabs/watermill/message"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func publishWithInTransaction(connectionDSN string) {
	var err error
	// create a new connection for each transaction
	// unless you guard it with a sync.Mutex
	conn, err := sqlite.OpenConn(connectionDSN)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	closer := sqlitex.Transaction(conn)
	defer func() {
		if closer(&err); err != nil {
			panic(err)
		}
	}()

	publisher, err := wmsqlitezombiezen.NewPublisher(
		conn,
		wmsqlitezombiezen.PublisherOptions{
			// schema must be initialized elsewhere before using
			// the publisher within the transaction
			InitializeSchema: false,
		},
	)
	if err != nil {
		panic(err)
	}

	msg := message.NewMessage(watermill.NewUUID(), []byte(`{"message": "Hello, world!"}`))
	if err = publisher.Publish("example_topic", msg); err != nil {
		panic(err)
	}
}
