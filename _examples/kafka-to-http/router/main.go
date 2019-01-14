package main

import (
	"bytes"
	"net/http"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	watermill_http "github.com/ThreeDotsLabs/watermill/message/infrastructure/http"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	brokers   = []string{"kafka:9092"}
	logger    = watermill.NewStdLogger(false, false)
	topicName = "kafka_to_http_example"
)

// passMessages passes the message along if its event type is one of acceptedTypes.
func passMessages(acceptedTypes ...string) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		msgEventType := msg.Metadata.Get("event_type")

		for _, typ := range acceptedTypes {
			if string(typ) == msgEventType {
				return message.Messages{msg}, nil
			}
		}

		return nil, nil
	}
}

func marshalMessage(topic string, msg *message.Message) (*http.Request, error) {
	return http.NewRequest(http.MethodPost, "http://webhooks_server:8001/"+topic, bytes.NewBuffer(msg.Payload))
}

func main() {
	publisher, err := watermill_http.NewPublisher(watermill_http.PublisherConfig{
		MarshalMessageFunc: marshalMessage,
	}, logger)
	if err != nil {
		panic(err)
	}

	subscriber, err := kafka.NewSubscriber(kafka.SubscriberConfig{
		Brokers: brokers,
	}, nil, kafka.DefaultMarshaler{}, logger)
	if err != nil {
		panic(err)
	}

	pubSub := message.NewPubSub(publisher, subscriber)

	router, err := message.NewRouter(message.RouterConfig{CloseTimeout: 5 * time.Second}, logger)
	if err != nil {
		panic(err)
	}

	router.AddHandler("foo", topicName, "foo", pubSub, passMessages("Foo"))
	router.AddHandler("foo_or_bar", topicName, "foo_or_bar", pubSub, passMessages("Foo", "Bar"))
	router.AddHandler("all", topicName, "all", pubSub, passMessages("Foo", "Bar", "Baz"))
	router.AddPlugin(plugin.SignalsHandler)

	err = router.Run()

}
