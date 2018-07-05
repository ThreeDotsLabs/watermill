package main

import (
	"github.com/roblaszczak/gooddd/message/handler/plugin"
	"github.com/roblaszczak/gooddd/message/handler/middleware"

	"github.com/roblaszczak/gooddd/message/handler"
	"github.com/roblaszczak/gooddd/message"

	_ "net/http/pprof"
	"github.com/roblaszczak/gooddd"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/marshal"
	kafka2 "github.com/roblaszczak/gooddd/message/infrastructure/kafka"
	"github.com/roblaszczak/gooddd/message/infrastructure/http"
	stdHttp "net/http"
	"encoding/json"
	"io/ioutil"
	"github.com/satori/go.uuid"
)

type request struct {
	Foo string
	Bar string
}

func main() {
	logger := gooddd.NewStdLogger(false, false)

	pub, err := kafka2.NewPublisher([]string{"localhost:9092"}, marshal.Json{})
	if err != nil {
		panic(err)
	}

	sub, err := http.NewSubscriber(":8080", func(topic string, r *stdHttp.Request) (message.Message, error) {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}

		p := request{}
		if err := json.Unmarshal(b, &p); err != nil {
			return nil, err
		}

		return message.NewDefault(uuid.NewV4().String(), p), nil
	})
	if err != nil {
		panic(err)
	}

	h, err := handler.NewHandler(
		handler.Config{
			ServerName:         "http_to_kafka",
			PublishEventsTopic: "http_events",
		},
		sub,
		pub,
	)
	if err != nil {
		panic(err)
	}
	h.Logger = logger

	h.AddMiddleware(
		middleware.AckOnSuccess,
		middleware.Recoverer,
		middleware.CorrelationID,
	)
	h.AddPlugin(plugin.SignalsHandler)

	h.AddHandler(
		"posts_counter",
		"/test",
		func(msg message.Message) (producedMessages []message.Message, err error) {
			return []message.Message{msg}, nil
		},
	)

	h.Run()
}
