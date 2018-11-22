package main

import (
	"encoding/json"
	"io/ioutil"
	stdHttp "net/http"
	_ "net/http/pprof"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/http"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

type GitlabWebhook struct {
	ObjectKind string `json:"object_kind"`
}

func main() {
	logger := watermill.NewStdLogger(true, true)

	kafkaPublisher, err := kafka.NewPublisher([]string{"localhost:9092"}, kafka.DefaultMarshaler{}, nil)
	if err != nil {
		panic(err)
	}

	httpSubscriber, err := http.NewSubscriber(":8080", func(topic string, request *stdHttp.Request) (*message.Message, error) {
		b, err := ioutil.ReadAll(request.Body)
		if err != nil {
			return nil, errors.Wrap(err, "cannot read body")
		}

		return message.NewMessage(uuid.NewV4().String(), b), nil
	}, logger)
	if err != nil {
		panic(err)
	}

	r, err := message.NewRouter(
		message.RouterConfig{},
		logger,
	)
	if err != nil {
		panic(err)
	}

	r.AddMiddleware(
		middleware.Recoverer,
		middleware.CorrelationID,
	)
	r.AddPlugin(plugin.SignalsHandler)

	err = r.AddHandler(
		"http_to_kafka",
		"/gitlab-webhooks", // this is the URL of our API
		"webhooks",
		message.NewPubSub(kafkaPublisher, httpSubscriber),
		func(msg *message.Message) ([]*message.Message, error) {
			webhook := GitlabWebhook{}

			// simple validation
			if err := json.Unmarshal(msg.Payload, &webhook); err != nil {
				return nil, errors.Wrap(err, "cannot unmarshal message")
			}
			if webhook.ObjectKind == "" {
				return nil, errors.New("empty object kind")
			}

			// just forward from http subscriber to kafka publisher
			return []*message.Message{msg}, nil
		},
	)
	if err != nil {
		panic(err)
	}

	go func() {
		// HTTP server needs to be started after router is ready.
		<-r.Running()
		httpSubscriber.StartHTTPServer()
	}()

	r.Run()
}
