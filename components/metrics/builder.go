package metrics

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
)

func AddPrometheusRouterMetrics(r *message.Router, prometheusRegistry *prometheus.Registry, namespace string, subsystem string) {
	builder := PrometheusMetricsBuilder{
		Namespace:          namespace,
		Subsystem:          subsystem,
		PrometheusRegistry: prometheusRegistry,
	}

	r.AddPublisherDecorators(builder.DecoratePublisher)
	r.AddSubscriberDecorators(builder.DecorateSubscriber)
	r.AddMiddleware(builder.NewRouterMiddleware().Middleware)
}

// PrometheusMetricsBuilder provides methods to decorate publishers, subscribers and handlers.
type PrometheusMetricsBuilder struct {
	// PrometheusRegistry may be filled with a pre-existing Prometheus registry, or left empty for the default registry.
	PrometheusRegistry *prometheus.Registry

	Namespace string
	Subsystem string
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

// could use some generics lol

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
