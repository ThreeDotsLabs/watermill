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

	numOutgoing := random.Intn(3) + 1
	outgoing := make([]*message.Message, numOutgoing)

	for i := 0; i < numOutgoing; i++ {
		outgoing[i] = msg.Copy()
	}
	return outgoing, nil
}

// produceMessages produces the incoming messages in delays of 50-100 milliseconds.
func produceMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte{})
		err := publisher.Publish("sub_topic", msg)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second * 5)
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

	go produceMessages(pubSub)

	if err := router.Run(context.Background()); err != nil {
		panic(err)
	}
}
