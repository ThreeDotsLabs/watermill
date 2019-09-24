package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	stdHttp "net/http"
	_ "net/http/pprof"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-http/pkg/http"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	kafkaAddr = flag.String("kafka", "localhost:9092", "The address of the kafka broker")
	httpAddr  = flag.String("http", ":8080", "The address for the http subscriber")
)

type Webhook struct {
	ObjectKind string `json:"object_kind"`
}

func main() {
	flag.Parse()
	logger := watermill.NewStdLogger(true, true)

	kafkaPublisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   []string{*kafkaAddr},
			Marshaler: kafka.DefaultMarshaler{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	httpSubscriber, err := http.NewSubscriber(
		*httpAddr,
		http.SubscriberConfig{
			UnmarshalMessageFunc: func(topic string, request *stdHttp.Request) (*message.Message, error) {
				b, err := ioutil.ReadAll(request.Body)
				if err != nil {
					return nil, errors.Wrap(err, "cannot read body")
				}

				return message.NewMessage(watermill.NewUUID(), b), nil
			},
		},
		logger,
	)
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

	r.AddHandler(
		"http_to_kafka",
		"/webhooks", // this is the URL of our API
		httpSubscriber,
		"webhooks", // this is the topic the message will be published to
		kafkaPublisher,
		func(msg *message.Message) ([]*message.Message, error) {
			webhook := Webhook{}

			if err := json.Unmarshal(msg.Payload, &webhook); err != nil {
				return nil, errors.Wrap(err, "cannot unmarshal message")
			}

			// Add simple validation
			if webhook.ObjectKind == "" {
				return nil, errors.New("empty object kind")
			}

			// Simply forward the message from HTTP Subscriber to Kafka Publisher
			return []*message.Message{msg}, nil
		},
	)

	go func() {
		// HTTP server needs to be started after router is ready.
		<-r.Running()
		_ = httpSubscriber.StartHTTPServer()
	}()

	_ = r.Run(context.Background())
}
