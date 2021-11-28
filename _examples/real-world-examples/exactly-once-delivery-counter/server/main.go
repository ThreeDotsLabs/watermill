package main

import (
	stdSQL "database/sql"
	"encoding/json"
	"log"
	"net/http"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-sql/pkg/sql"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	driver "github.com/go-sql-driver/mysql"
)

const topic = "counter"

func main() {
	db := createDB()
	logger := watermill.NewStdLogger(false, false)

	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.Logger)

	publisher, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			SchemaAdapter: sql.DefaultMySQLSchema{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	r.Post("/count/{counterUUID}", func(w http.ResponseWriter, r *http.Request) {
		payload, err := json.Marshal(messagePayload{
			CounterUUID: chi.URLParam(r, "counterUUID"),
		})
		if err != nil {
			log.Print(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		msg := message.NewMessage(watermill.NewUUID(), payload)

		if err := publisher.Publish(topic, msg); err != nil {
			log.Print(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	http.ListenAndServe(":8080", r)
}

type messagePayload struct {
	CounterUUID string `json:"counter_uuid"`
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
