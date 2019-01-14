package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	watermill_http "github.com/ThreeDotsLabs/watermill/message/infrastructure/http"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"

	uuid "github.com/satori/go.uuid"
)

var (
	brokers = []string{"localhost:9092"}
	logger  = watermill.NewStdLogger(false, false)

	topicName            = "kafka_to_http_example"
	eventTypeMetadataKey = "eventType"
)

type eventType string

const (
	Foo eventType = "Foo"
	Bar eventType = "Bar"
	Baz eventType = "Baz"
)

func produceMessages(howMany int) {
	pub, err := kafka.NewPublisher(brokers, kafka.DefaultMarshaler{}, nil, logger)
	if err != nil {
		panic(err)
	}

	eventTypes := []eventType{Foo, Bar, Baz}

	for i := 0; i < howMany; i++ {
		eventType := eventTypes[i%3]
		msg := message.NewMessage(uuid.NewV4().String(), []byte("message"))
		msg.Metadata.Set(eventTypeMetadataKey, string(eventType))

		fmt.Printf("Publishing kafka message %s\n", eventType)
		if err := pub.Publish(topicName, msg); err != nil {
			panic(err)
		}

		time.Sleep(200 * time.Millisecond)
	}
}

// testServer receives the webhook requests and logs them in stdout.
func testServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Printf(
			"[%s] %s %s: %s\n",
			time.Now().String(),
			r.Method,
			r.URL.String(),
			string(body),
		)
	}))
}

// passMessages passes the message along if its event type is one of acceptedTypes.
func passMessages(acceptedTypes ...eventType) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		var passedMessages []*message.Message
		msgEventType := msg.Metadata.Get(eventTypeMetadataKey)

		for _, typ := range acceptedTypes {
			if string(typ) == msgEventType {
				passedMessages = append(passedMessages, msg)
				return passedMessages, nil
			}
		}

		return nil, nil
	}
}

func main() {
	ts := testServer()
	defer ts.Close()

	marshallMessage := func(topic string, msg *message.Message) (*http.Request, error) {
		return http.NewRequest(http.MethodPost, ts.URL+"/"+topic, bytes.NewBuffer(msg.Payload))
	}

	publisher, err := watermill_http.NewPublisher(watermill_http.PublisherConfig{
		MarshalMessageFunc: marshallMessage,
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

	router, err := message.NewRouter(message.RouterConfig{
		CloseTimeout: 5 * time.Second,
	}, logger)
	if err != nil {
		panic(err)
	}

	router.AddHandler("foo", topicName, "foo", pubSub, passMessages(Foo))
	router.AddHandler("foo_or_bar", topicName, "foo_or_bar", pubSub, passMessages(Foo, Bar))
	router.AddHandler("all", topicName, "all", pubSub, passMessages(Foo, Bar, Baz))

	router.AddPlugin(plugin.SignalsHandler)

	go func() {
		err = router.Run()
		if err != nil {
			panic(err)
		}
	}()

	produceMessages(100)
	err = router.Close()
	if err != nil {
		panic(err)
	}

	fmt.Print("router run end\n")
	<-router.Running()
	fmt.Print("router running end\n")
}
