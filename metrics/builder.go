package metrics

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
)

func AddPrometheusRouterMetrics(r *message.Router, prometheusRegistry *prometheus.Registry, namespace string, subsystem string) {
	r.AddPublisherDecorators(PrometheusPublisherMetricsBuilder{
		Namespace:          namespace,
		Subsystem:          subsystem,
		PrometheusRegistry: prometheusRegistry,
	}.Decorate)
}
