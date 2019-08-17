package main

import (
	"context"
	stdSQL "database/sql"
	"encoding/json"
	"log"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

var (
	logger       = watermill.NewStdLogger(true, false)
	consumeTopic = "events"
	publishTopic = "topic"
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

type event struct {
	Num int `json:"num"`
}

func publishEvents(db *stdSQL.DB) {
	i := 0
	for {
		payload, err := json.Marshal(event{Num: int(time.Now().Unix())})
		if err != nil {
			panic(err)
		}

		_, err = db.Exec("INSERT INTO watermill_events (payload, uuid) VALUES (?, ?)",
			payload, watermill.NewUUID())
		if err != nil {
			panic(err)
		}

		i++
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

	goChannel := gochannel.NewGoChannel(gochannel.Config{}, logger)
	subscriber := createSubscriber(db)

	router.AddHandler(
		"mysql-to-gochannel",
		consumeTopic,
		subscriber,
		publishTopic,
		goChannel,
		func(msg *message.Message) ([]*message.Message, error) {
			consumedPayload := event{}
			err := json.Unmarshal(msg.Payload, &consumedPayload)
			if err != nil {
				return nil, err
			}

			log.Printf("received event %d with UUID %s", consumedPayload.Num, msg.UUID)

			return []*message.Message{msg}, nil
		},
	)

	go func() {
		ctx := context.Background()
		if err := router.Run(ctx); err != nil {
			panic(err)
		}
	}()

	<-router.Running()
	publishEvents(db)
}
