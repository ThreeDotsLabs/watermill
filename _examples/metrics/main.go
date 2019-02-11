package main

import (
	"context"
	"flag"
	"math"
	"math/rand"
	"time"

	"github.com/pkg/errors"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/metrics"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

var (
	metricsAddr  = flag.String("metrics", ":8081", "The address that will expose /metrics for Prometheus")
	handlerDelay = flag.Float64("delay", 0, "The stdev of normal distribution of delay in handler (in seconds), to simulate load")

	logger = watermill.NewStdLogger(true, true)
	random = rand.New(rand.NewSource(time.Now().Unix()))
)

func delay() {
	seconds := *handlerDelay
	if seconds == 0 {
		return
	}
	delay := math.Abs(random.NormFloat64() * seconds)
	time.Sleep(time.Duration(float64(time.Second) * delay))
}

// handler publishes 0-4 messages with a random delay.
func handler(msg *message.Message) ([]*message.Message, error) {
	delay()

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

// produceMessages produces the incoming messages in delays of 500-1000 milliseconds.
func produceMessages(routerClosed chan struct{}, publisher message.Publisher) {
	for {
		select {
		case <-routerClosed:
			return
		default:
			// go on
		}

		time.Sleep(500*time.Millisecond + time.Duration(random.Intn(500))*time.Millisecond)
		msg := message.NewMessage(watermill.UUID(), []byte{})
		_ = publisher.Publish("sub_topic", msg)
	}
}

func main() {
	flag.Parse()

	pubSub := gochannel.NewGoChannel(0, logger)

	r, err := message.NewRouter(
		message.RouterConfig{},
		logger,
	)
	if err != nil {
		panic(err)
	}

	prometheusRegistry := prometheus.NewRegistry()
	closeMetrics := metrics.ServeHTTP(*metricsAddr, prometheusRegistry)
	defer closeMetrics()
	metricsBuilder := metrics.NewPrometheusMetricsBuilder(prometheusRegistry, "", "")
	metricsBuilder.AddPrometheusRouterMetrics(r)

	r.AddMiddleware(
		middleware.Recoverer,
		middleware.RandomFail(0.1),
		middleware.RandomPanic(0.1),
	)
	r.AddPlugin(plugin.SignalsHandler)

	err = r.AddHandler(
		"metrics-example",
		"sub_topic",
		pubSub,
		"pub_topic",
		pubSub,
		handler,
	)
	if err != nil {
		panic(err)
	}

	pubWithRandomFail := randomFailPublisherDecorator{pubSub, 0.1}

	// The handler's publisher and subscriber will be decorated by `AddPrometheusRouterMetrics`.
	// but we will use the same pub/sub to generate messages incoming to the handler
	// and consume the outgoing messages.
	// They will have `handler_name=<no handler>` label in Prometheus.
	subWithMetrics, err := metricsBuilder.DecorateSubscriber(pubSub)
	if err != nil {
		panic(err)
	}
	pubWithMetrics, err := metricsBuilder.DecoratePublisher(pubWithRandomFail)
	if err != nil {
		panic(err)
	}

	routerClosed := make(chan struct{})
	go produceMessages(routerClosed, pubWithMetrics)
	go consumeMessages(subWithMetrics)

	_ = r.Run()
	close(routerClosed)
}

type randomFailPublisherDecorator struct {
	message.Publisher
	failProbability float64
}

func (r randomFailPublisherDecorator) Publish(topic string, messages ...*message.Message) error {
	if random.Float64() < r.failProbability {
		return errors.New("random publishing failure")
	}
	return r.Publisher.Publish(topic, messages...)
}
