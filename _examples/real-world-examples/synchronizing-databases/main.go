package main

import (
	"bytes"
	"context"
	stdSQL "database/sql"
	"encoding/gob"
	"fmt"
	"log"
	"time"

	"github.com/brianvoe/gofakeit"
	driver "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	logger        = watermill.NewStdLogger(false, false)
	postgresTable = "users"
	mysqlTable    = "users"
)

func main() {
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)

	mysqlDB := createMySQLConnection()
	postgresDB := createPostgresConnection()

	subscriber := createSubscriber(mysqlDB)
	publisher := createPublisher(postgresDB)

	go simulateEvents(mysqlDB)

	router.AddHandler(
		"mysql-to-postgres",
		mysqlTable,
		subscriber,
		postgresTable,
		publisher,
		func(msg *message.Message) ([]*message.Message, error) {
			originUser := mysqlUser{}

			decoder := gob.NewDecoder(bytes.NewBuffer(msg.Payload))
			err := decoder.Decode(&originUser)
			if err != nil {
				return nil, err
			}

			log.Printf("received user: %+v", originUser)

			newUser := postgresUser{
				ID:        originUser.ID,
				Username:  originUser.User,
				FullName:  fmt.Sprintf("%s %s", originUser.FirstName, originUser.LastName),
				CreatedAt: originUser.CreatedAt,
			}

			var payload bytes.Buffer
			encoder := gob.NewEncoder(&payload)
			err = encoder.Encode(newUser)
			if err != nil {
				return nil, err
			}

			newMessage := message.NewMessage(watermill.NewULID(), payload.Bytes())

			return []*message.Message{newMessage}, nil
		},
	)

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}

func createMySQLConnection() *stdSQL.DB {
	conf := driver.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = "mysql"
	conf.DBName = "watermill"
	conf.ParseTime = true

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

func createPostgresConnection() *stdSQL.DB {
	dsn := "postgres://watermill:password@postgres/watermill?sslmode=disable"
	db, err := stdSQL.Open("postgres", dsn)
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
	sub, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			SchemaAdapter:    mysqlSchemaAdapter{},
			OffsetsAdapter:   sql.DefaultMySQLOffsetsAdapter{},
			InitializeSchema: true,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	return sub
}

func createPublisher(db *stdSQL.DB) message.Publisher {
	pub, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			SchemaAdapter:        postgresSchemaAdapter{},
			AutoInitializeSchema: true,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	return pub
}

func simulateEvents(db *stdSQL.DB) {
	pub, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			SchemaAdapter:        mysqlSchemaAdapter{},
			AutoInitializeSchema: true,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	for {
		user := mysqlUser{
			User:      gofakeit.Username(),
			FirstName: gofakeit.FirstName(),
			LastName:  gofakeit.LastName(),
			CreatedAt: time.Now().UTC(),
		}

		var payload bytes.Buffer
		encoder := gob.NewEncoder(&payload)
		err := encoder.Encode(user)
		if err != nil {
			panic(err)
		}

		err = pub.Publish(mysqlTable, message.NewMessage(
			watermill.NewUUID(),
			payload.Bytes(),
		))
		if err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}
