package main

import (
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

// filterMessages passes the message along if its event type is one of acceptedTypes.
func filterMessages(acceptedTypes ...string) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		// the kafka producer sets this metadata so that we don't have to unmarshal the body
		// just sort the messages based on event type metadata
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
	return watermill_http.DefaultMarshalMessageFunc("http://webhooks-server:8001/"+topic, msg)
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

	router.AddHandler("foo", topicName, "foo", pubSub, filterMessages("Foo"))
	router.AddHandler("foo_or_bar", topicName, "foo_or_bar", pubSub, filterMessages("Foo", "Bar"))
	router.AddHandler("all", topicName, "all", pubSub, filterMessages("Foo", "Bar", "Baz"))
	router.AddPlugin(plugin.SignalsHandler)

	err = router.Run()
}
