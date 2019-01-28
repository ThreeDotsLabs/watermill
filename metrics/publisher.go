package metrics

import (
	"net/http"
	"time"

	multierror "github.com/hashicorp/go-multierror"

	"github.com/go-chi/chi"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	publisherLabelKeys = []string{
		labelKeyHandlerName,
		labelKeyPublisherName,
	}
)

type PublisherPrometheusMetricsDecorator struct {
	pub        message.Publisher
	httpServer http.Server

	publisherSuccessTotal *prometheus.CounterVec
	publisherFailTotal    *prometheus.CounterVec
	publishTimeSeconds    *prometheus.HistogramVec
	publisherCountTotal   prometheus.Gauge
}

// Publish updates the relevant publisher metrics and calls the wrapped publisher's Publish.
func (m PublisherPrometheusMetricsDecorator) Publish(topic string, messages ...*message.Message) (err error) {
	if len(messages) == 0 {
		return m.pub.Publish(topic)
	}

	// TODO: take ctx not only from first msg. Might require changing the signature of Publish, which is planned anyway.
	ctx := messages[0].Context()
	labels := labelsFromCtx(ctx, publisherLabelKeys...)
	now := time.Now()

	defer func() {
		m.publishTimeSeconds.With(labels).Observe(time.Since(now).Seconds())
	}()
	defer func() {
		if err != nil {
			m.publisherFailTotal.With(labels).Inc()
			return
		}
		m.publisherSuccessTotal.With(labels).Inc()
	}()
	return m.pub.Publish(topic, messages...)
}

// Close decreases the total publisher count, closes the Prometheus HTTP server and calls wrapped Close.
func (m PublisherPrometheusMetricsDecorator) Close() error {
	m.publisherCountTotal.Dec()

	err := m.httpServer.Close()

	if closeErr := m.pub.Close(); closeErr != nil {
		err = multierror.Append(err, closeErr)
	}

	return err
}

// PrometheusPublisherMetricsBuilder provides Decorate method, which is a publisher decorator.
type PrometheusPublisherMetricsBuilder struct {
	// PrometheusRegistry may be filled with a pre-existing Prometheus registry, or left empty for the default registry.
	PrometheusRegistry *prometheus.Registry
	// HandlerOpts may be supplied according to promhttp, or left empty for default values.
	HandlerOpts promhttp.HandlerOpts

	Namespace string
	Subsystem string

	// PrometheusBindAddress is the address at which the Prometheus /metrics endpoint will be served.
	PrometheusBindAddress string
}

// Decorate wraps the underlying publisher with Prometheus metrics.
func (b PrometheusPublisherMetricsBuilder) Decorate(pub message.Publisher) (wrapped message.Publisher, err error) {
	prometheusRegistry := b.PrometheusRegistry
	if prometheusRegistry == nil {
		prometheusRegistry = prometheus.NewRegistry()
	}

	publishSuccessTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "publisher_success_total",
			Help:      "Total number of successfully produced messages",
		},
		publisherLabelKeys,
	)

	publishFailTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "publisher_fail_total",
			Help:      "Total number of failed attempts to publish a message",
		},
		publisherLabelKeys,
	)

	publishTimeSeconds := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "publish_time_seconds",
			Help:      "The time that a publishing attempt (success or not) took in seconds",
		},
		publisherLabelKeys,
	)

	publisherCountTotal := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "publisher_count_total",
			Help:      "The total count of active publishers",
		},
	)

	for _, c := range []prometheus.Collector{
		publishSuccessTotal,
		publishFailTotal,
		publishTimeSeconds,
		// publisherCountTotal is WIP, don't register yet
	} {
		if registerErr := prometheusRegistry.Register(c); registerErr != nil {
			err = multierror.Append(err, registerErr)
		}
	}
	if err != nil {
		return nil, err
	}

	// todo: just register metrics on the registry. leave the http server to someone outside the decorator.
	router := chi.NewRouter()
	handler := promhttp.HandlerFor(prometheusRegistry, b.HandlerOpts)
	router.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
	})
	server := http.Server{
		Addr:    b.PrometheusBindAddress,
		Handler: handler,
	}

	go func() {
		err := server.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()

	publisherCountTotal.Inc()
	return PublisherPrometheusMetricsDecorator{
		pub,
		server,
		publishSuccessTotal,
		publishFailTotal,
		publishTimeSeconds,
		publisherCountTotal,
	}, nil
}
