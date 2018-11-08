package main

import (
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"

	"github.com/ThreeDotsLabs/watermill/message"

	"encoding/json"
	"io/ioutil"
	stdHttp "net/http"
	_ "net/http/pprof"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/http"
	kafka2 "github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka/marshal"
	"github.com/satori/go.uuid"
)

type request struct {
	Foo string
	Bar string
}

func main() {
	logger := watermill.NewStdLogger(false, false)

	pub, err := kafka2.NewPublisher([]string{"localhost:9092"}, marshal.ConfluentKafka{})
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

		return message.NewMessage(uuid.NewV4().String(), p), nil
	}, logger)
	if err != nil {
		panic(err)
	}

	h, err := message.NewRouter(
		message.RouterConfig{
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
