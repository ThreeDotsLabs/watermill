package metrics

import (
	"github.com/ThreeDotsLabs/watermill/internal"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type PrometheusMetricsBuilderConfig struct {
	Namespace        string
	Subsystem        string
	AdditionalLabels []MetricLabel
}

func NewPrometheusMetricsBuilderWithConfig(prometheusRegistry prometheus.Registerer, config PrometheusMetricsBuilderConfig) PrometheusMetricsBuilder {
	builder := PrometheusMetricsBuilder{
		Namespace:          config.Namespace,
		Subsystem:          config.Subsystem,
		PrometheusRegistry: prometheusRegistry,
		additionalLabels:   config.AdditionalLabels,
	}
	return builder
}

func NewPrometheusMetricsBuilder(prometheusRegistry prometheus.Registerer, namespace string, subsystem string) PrometheusMetricsBuilder {
	return NewPrometheusMetricsBuilderWithConfig(prometheusRegistry, PrometheusMetricsBuilderConfig{
		Namespace: namespace,
		Subsystem: subsystem,
	})
}

// PrometheusMetricsBuilder provides methods to decorate publishers, subscribers and handlers.
type PrometheusMetricsBuilder struct {
	// PrometheusRegistry may be filled with a pre-existing Prometheus registry, or left empty for the default registry.
	PrometheusRegistry prometheus.Registerer

	Namespace string
	Subsystem string

	additionalLabels []MetricLabel
}

// AddPrometheusRouterMetrics is a convenience function that acts on the message router to add the metrics middleware
// to all its handlers. The handlers' publishers and subscribers are also decorated.
func (b PrometheusMetricsBuilder) AddPrometheusRouterMetrics(r *message.Router) {
	r.AddPublisherDecorators(b.DecoratePublisher)
	r.AddSubscriberDecorators(b.DecorateSubscriber)
	r.AddMiddleware(b.NewRouterMiddleware().Middleware)
}

// DecoratePublisher wraps the underlying publisher with Prometheus metrics.
func (b PrometheusMetricsBuilder) DecoratePublisher(pub message.Publisher) (message.Publisher, error) {
	var err error
	d := PublisherPrometheusMetricsDecorator{
		pub:              pub,
		publisherName:    internal.StructName(pub),
		additionalLabels: b.additionalLabels,
	}

	d.publishTimeSeconds, err = b.registerHistogramVec(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "publish_time_seconds",
			Help:      "The time that a publishing attempt (success or not) took in seconds",
		},
		toLabelsSlice(publisherLabelKeys, b.additionalLabels),
	))
	if err != nil {
		return nil, errors.Wrap(err, "could not register publish time metric")
	}
	return d, nil
}

// DecorateSubscriber wraps the underlying subscriber with Prometheus metrics.
func (b PrometheusMetricsBuilder) DecorateSubscriber(sub message.Subscriber) (message.Subscriber, error) {
	var err error
	d := &SubscriberPrometheusMetricsDecorator{
		closing:          make(chan struct{}),
		subscriberName:   internal.StructName(sub),
		additionalLabels: b.additionalLabels,
	}

	d.subscriberMessagesReceivedTotal, err = b.registerCounterVec(prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_messages_received_total",
			Help:      "The total number of messages received by the subscriber",
		},
		toLabelsSlice(append(subscriberLabelKeys, labelAcked), b.additionalLabels),
	))
	if err != nil {
		return nil, errors.Wrap(err, "could not register time to ack metric")
	}

	d.Subscriber, err = message.MessageTransformSubscriberDecorator(d.recordMetrics)(sub)
	if err != nil {
		return nil, errors.Wrap(err, "could not decorate subscriber with metrics decorator")
	}

	return d, nil
}

func (b PrometheusMetricsBuilder) register(c prometheus.Collector) (prometheus.Collector, error) {
	err := b.PrometheusRegistry.Register(c)
	if err == nil {
		return c, nil
	}

	if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
		return are.ExistingCollector, nil
	}

	return nil, err
}

func (b PrometheusMetricsBuilder) registerCounterVec(c *prometheus.CounterVec) (*prometheus.CounterVec, error) {
	col, err := b.register(c)
	if err != nil {
		return nil, err
	}
	return col.(*prometheus.CounterVec), nil
}

func (b PrometheusMetricsBuilder) registerHistogramVec(h *prometheus.HistogramVec) (*prometheus.HistogramVec, error) {
	col, err := b.register(h)
	if err != nil {
		return nil, err
	}
	return col.(*prometheus.HistogramVec), nil
}
