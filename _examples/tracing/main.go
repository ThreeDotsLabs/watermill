package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/tracing"

	"go.opencensus.io/exporter/jaeger"
	"go.opencensus.io/trace"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	logger = watermill.NewStdLogger(true, true)
	random = rand.New(rand.NewSource(time.Now().Unix()))
)

// handler publishes 0-4 messages with a random delay.
func handler(msg *message.Message) ([]*message.Message, error) {
	// todo - doc https://github.com/census-instrumentation/opencensus-go#spans

	_, s1 := trace.StartSpan(msg.Context(), "sleep_300")
	time.Sleep(time.Millisecond * 300)
	s1.End()

	_, s2 := trace.StartSpan(msg.Context(), "sleep_600")
	time.Sleep(time.Millisecond * 600)
	s2.End()

	numOutgoing := random.Intn(4)
	outgoing := make([]*message.Message, numOutgoing)

	for i := 0; i < numOutgoing; i++ {
		outgoing[i] = msg.Copy()
	}
	return outgoing, nil
}

// consumeMessages consumes the messages exiting the handler.
func consumeMessages(subscriber message.Subscriber) {
	messages, err := subscriber.Subscribe(context.Background(), "pub_topic")
	if err != nil {
		panic(err)
	}

	for msg := range messages {
		msg.Ack()
	}
}

// produceMessages produces the incoming messages in delays of 50-100 milliseconds.
func produceMessages(routerClosed chan struct{}, publisher message.Publisher) {
	for {
		select {
		case <-routerClosed:
			return
		default:
			// go on
		}

		time.Sleep(50*time.Millisecond + time.Duration(random.Intn(50))*time.Millisecond)
		msg := message.NewMessage(watermill.NewUUID(), []byte{})
		_ = publisher.Publish("sub_topic", msg)
	}
}

func main() {
	flag.Parse()

	agentEndpointURI := "localhost:6831"
	collectorEndpointURI := "http://localhost:14268/api/traces"

	je, err := jaeger.NewExporter(jaeger.Options{
		AgentEndpoint:     agentEndpointURI,
		CollectorEndpoint: collectorEndpointURI,
		ServiceName:       "demo",
	})
	if err != nil {
		log.Fatalf("Failed to create the Jaeger exporter: %v", err)
	}

	// And now finally register it as a Trace Exporter
	trace.RegisterExporter(je)

	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)

	//pub := tracing.DecoratePublisher(pubSub)
	//sub := tracing.DecorateSubscriber(pubSub)

	router, err := message.NewRouter(
		message.RouterConfig{},
		logger,
	)
	if err != nil {
		panic(err)
	}

	router.AddPublisherDecorators(
		tracing.DecoratePublisher(),
	)
	router.AddSubscriberDecorators(
		tracing.DecorateSubscriber(),
	)
	router.AddMiddleware(tracing.Middleware())

	router.AddPlugin(plugin.SignalsHandler)

	router.AddHandler(
		"metrics-example",
		"sub_topic",
		pubSub,
		"pub_topic",
		pubSub,
		handler,
	)

	router.AddNoPublisherHandler(
		"metrics-example-2",
		"pub_topic",
		pubSub,
		func(msg *message.Message) error {
			time.Sleep(time.Millisecond * 500)
			return nil
		},
	)

	// separate the publisher from pubSub to decorate it separately

	routerClosed := make(chan struct{})
	go produceMessages(routerClosed, pubSub)
	go consumeMessages(pubSub)

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}
