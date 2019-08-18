package main

import (
	"context"
	stdSQL "database/sql"
	"encoding/json"
	"log"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	kafkaBrokers = []string{"kafka:9092"}
	logger       = watermill.NewStdLogger(false, false)
	consumeTopic = "events"
	publishTopic = "events"
)

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
			SchemaAdapter:    sql.DefaultSchema{},
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
		kafkaBrokers,
		kafka.DefaultMarshaler{},
		nil,
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

func publishEvents(db *stdSQL.DB) {
	for {
		e := event{
			Name:       "UserSignedUp",
			OccurredAt: time.Now().UTC().Format(time.RFC3339),
		}
		payload, err := json.Marshal(e)
		if err != nil {
			panic(err)
		}

		_, err = db.Exec("INSERT INTO watermill_events (payload, uuid) VALUES (?, ?)",
			payload, watermill.NewUUID())
		if err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

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
		consumeTopic,
		subscriber,
		publishTopic,
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			consumedPayload := event{}
			err := json.Unmarshal(msg.Payload, &consumedPayload)
			if err != nil {
				return nil, err
			}

			log.Printf("received event %+v with UUID %s", consumedPayload, msg.UUID)

			return []*message.Message{msg}, nil
		},
	)

	go func() {
		<-router.Running()
		publishEvents(db)
	}()

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}
