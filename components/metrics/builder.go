package metrics

import (
	"github.com/ThreeDotsLabs/watermill/internal"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

func NewPrometheusMetricsBuilder(prometheusRegistry *prometheus.Registry, namespace string, subsystem string) PrometheusMetricsBuilder {
	return PrometheusMetricsBuilder{
		Namespace:          namespace,
		Subsystem:          subsystem,
		PrometheusRegistry: prometheusRegistry,
	}
}

// PrometheusMetricsBuilder provides methods to decorate publishers, subscribers and handlers.
type PrometheusMetricsBuilder struct {
	// PrometheusRegistry may be filled with a pre-existing Prometheus registry, or left empty for the default registry.
	PrometheusRegistry *prometheus.Registry

	Namespace string
	Subsystem string
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
		pub:           pub,
		publisherName: internal.StructName(pub),
	}

	d.publishTimeSeconds, err = b.registerHistogramVec(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "publish_time_seconds",
			Help:      "The time that a publishing attempt (success or not) took in seconds",
		},
		publisherLabelKeys,
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
		closing:        make(chan struct{}),
		subscriberName: internal.StructName(sub),
	}

	d.subscriberMessagesReceivedTotal, err = b.registerCounterVec(prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_messages_received_total",
			Help:      "The total number of messages received by the subscriber",
		},
		append(subscriberLabelKeys, labelAcked),
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
