package main

import (
	"context"
	stdSQL "database/sql"
	"encoding/json"
	"log"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	logger     = watermill.NewStdLogger(false, false)
	kafkaTopic = "events"
	mysqlTable = "events"
)

func main() {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)

	db := createDB()

	subscriber := createSubscriber(db)
	publisher := createPublisher()

	router.AddHandler(
		"mysql-to-kafka",
		mysqlTable,
		subscriber,
		kafkaTopic,
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			consumedEvent := event{}
			err := json.Unmarshal(msg.Payload, &consumedEvent)
			if err != nil {
				return nil, err
			}

			log.Printf("received event %+v with UUID %s", consumedEvent, msg.UUID)

			return []*message.Message{msg}, nil
		},
	)

	go func() {
		<-router.Running()
		simulateEvents(db)
	}()

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}

func createDB() *stdSQL.DB {
	conf := driver.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = "mysql"
	conf.DBName = "watermill"

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

func createSubscriber(db *stdSQL.DB) message.Subscriber {
	pub, err := sql.NewSubscriber(
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

	return pub
}

func createPublisher() message.Publisher {
	pub, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   []string{"kafka:9092"},
			Marshaler: kafka.DefaultMarshaler{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	return pub
}

type event struct {
	Name       string `json:"name"`
	OccurredAt string `json:"occurred_at"`
}

func simulateEvents(db *stdSQL.DB) {
	for {
		tx, err := db.Begin()
		if err != nil {
			panic(err)
		}

		// In an actual application, this is the place where some aggreagte would be persisted
		// using the same transaction.
		// tx.Exec("INSERT INTO (...)")

		err = publishEvent(tx)
		if err != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				panic(rollbackErr)
			}
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

// publishEvent publishes a new event.
// To publish the event in a separate transaction, a new SQL Publisher
// has to be created each time, passing the proper transaction handle.
func publishEvent(tx *stdSQL.Tx) error {
	pub, err := sql.NewPublisher(tx, sql.PublisherConfig{
		SchemaAdapter: sql.DefaultMySQLSchema{},
	}, logger)
	if err != nil {
		return err
	}

	e := event{
		Name:       "UserSignedUp",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}

	return pub.Publish(mysqlTable, message.NewMessage(
		watermill.NewUUID(),
		payload,
	))
}
