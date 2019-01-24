package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ThreeDotsLabs/watermill/message"
)

type PublisherPrometheusMetricsMiddleware struct {
	pub message.Publisher

	publisherSuccessTotal *prometheus.CounterVec
	publisherFailTotal    *prometheus.CounterVec
	publishTimeSeconds    *prometheus.HistogramVec
}

func (m PublisherPrometheusMetricsMiddleware) Publish(topic string, messages ...*message.Message) (err error) {
	if len(messages) == 0 {
		return m.pub.Publish(topic)
	}

	// TODO: take ctx not only from first msg. Might require changing the signature of Publish, which is planned anyway.
	ctx := messages[0].Context()
	labels := labelsFromCtx(ctx)
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

func (m PublisherPrometheusMetricsMiddleware) Close() error {
	return m.pub.Close()
}

type PrometheusPublisherMetricsBuilder struct {
	PrometheusRegistry *prometheus.Registry
	Namespace          string
	Subsystem          string
}

func (b PrometheusPublisherMetricsBuilder) Decorate(pub message.Publisher) message.Publisher {
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
		labelKeys,
	)

	publishFailTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "publisher_fail_total",
			Help:      "Total number of failed attempts to publish a message",
		},
		labelKeys,
	)

	publishTimeSeconds := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "publish_time_seconds",
			Help:      "The time that a publishing attempt (success or not) took in seconds",
		},
		labelKeys,
	)

	prometheusRegistry.MustRegister(publishSuccessTotal, publishFailTotal, publishTimeSeconds)

	go func() {
		// todo: stop the server eventually. configure the address.
		http.Handle("/metrics", promhttp.HandlerFor(prometheusRegistry, promhttp.HandlerOpts{}))
		err := http.ListenAndServe(":8081", nil)
		if err != http.ErrServerClosed {
			fmt.Printf("Server exited with err %+v", err)
		}
	}()

	return PublisherPrometheusMetricsMiddleware{
		pub,
		publishSuccessTotal,
		publishFailTotal,
		publishTimeSeconds,
	}
}
