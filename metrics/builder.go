package metrics

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

func AddPrometheusRouterMetrics(r *message.Router, namespace string, subsystem string) {
	r.AddPublisherDecorators(PrometheusPublisherMetricsBuilder{
		Namespace:             namespace,
		Subsystem:             subsystem,
		PrometheusBindAddress: ":8081",
	}.Decorate)
}
