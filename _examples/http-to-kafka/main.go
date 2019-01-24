package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	stdHttp "net/http"
	_ "net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/http"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"

	prometheusmetrics "github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	metrics "github.com/rcrowley/go-metrics"
)

type GitlabWebhook struct {
	ObjectKind string `json:"object_kind"`
}

type PublisherMetrics struct {
	pub message.Publisher

	publishedSuccess metrics.Counter
	publishedFail    metrics.Counter
}

func (m PublisherMetrics) Publish(topic string, messages ...*message.Message) (err error) {
	defer func() {
		if err != nil {
			m.publishedFail.Inc(1)
			return
		}
		m.publishedSuccess.Inc(1)
	}()
	return m.pub.Publish(topic, messages...)
}

func (m PublisherMetrics) Close() error {
	return m.pub.Close()
}

type PrometheusPublisherMetricsBuilder struct {
	MetricsRegistry    metrics.Registry
	PrometheusRegistry *prometheus.Registry
	PrometheusConfig   *prometheusmetrics.PrometheusConfig
}

func (b PrometheusPublisherMetricsBuilder) Decorate(pub message.Publisher) message.Publisher {
	metricsRegistry := b.MetricsRegistry
	if metricsRegistry == nil {
		metricsRegistry = metrics.NewRegistry()
	}

	prometheusRegistry := b.PrometheusRegistry
	if prometheusRegistry == nil {
		prometheusRegistry = prometheus.NewRegistry()
	}

	pClient := b.PrometheusConfig
	if pClient == nil {
		// todo: some sensible defaults?
		pClient = prometheusmetrics.NewPrometheusProvider(metricsRegistry, "http-to-kafka", "", prometheusRegistry, time.Second)
	}

	go pClient.UpdatePrometheusMetrics()

	publishSuccess := metrics.NewRegisteredCounter("published_success", metricsRegistry)
	publishFail := metrics.NewRegisteredCounter("published_fail", metricsRegistry)

	go func() {
		stdHttp.Handle("/metrics", promhttp.HandlerFor(prometheusRegistry, promhttp.HandlerOpts{}))
		err := stdHttp.ListenAndServe(":8081", nil)
		if err != stdHttp.ErrServerClosed {
			fmt.Printf("Server exited with err %+v", err)
		}
	}()

	return PublisherMetrics{pub, publishSuccess, publishFail}
}

func DecorateRouterWithMetrics(r *message.Router) {
	r.AddPublisherDecorators(PrometheusPublisherMetricsBuilder{}.Decorate)
}

func main() {
	logger := watermill.NewStdLogger(true, true)

	kafkaPublisher, err := kafka.NewPublisher([]string{"localhost:9092"}, kafka.DefaultMarshaler{}, nil, logger)
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
		middleware.RandomPanic(0.1),
	)
	r.AddPlugin(plugin.SignalsHandler)

	DecorateRouterWithMetrics(r)

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
		_ = httpSubscriber.StartHTTPServer()
	}()

	_ = r.Run()
}
