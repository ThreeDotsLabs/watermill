package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	labelKeys = []string{
		"pubsub_provider",
		"handler_name",
		"hostname",
	}
)

func labelsFromCtx(ctx context.Context) prometheus.Labels {
	labels := map[string]string{}
	for _, key := range labelKeys {
		val, ok := ctx.Value(key).(string)
		if !ok {
			labels[key] = ""
		}
		labels[key] = val
	}
	return labels
}
