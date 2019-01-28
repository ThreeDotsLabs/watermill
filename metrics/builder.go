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
}

// PrometheusMetricsBuilder provides methods to decorate publishers, subscribers and handlers.
type PrometheusMetricsBuilder struct {
	// PrometheusRegistry may be filled with a pre-existing Prometheus registry, or left empty for the default registry.
	PrometheusRegistry *prometheus.Registry

	Namespace string
	Subsystem string
}
