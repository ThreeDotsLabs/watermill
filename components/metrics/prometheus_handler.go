package metrics

import (
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ThreeDotsLabs/watermill/message"
)

// HandlerPrometheusMetricsMiddleware is a middleware that captures Prometheus metrics.
type HandlerPrometheusMetricsMiddleware struct {
	handlerExecutionTimeSeconds *prometheus.HistogramVec
}

// Middleware returns the middleware ready to be used with watermill's Router.
func (m HandlerPrometheusMetricsMiddleware) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (msgs []*message.Message, err error) {
		now := time.Now()
		ctx := msg.Context()
		labels := prometheus.Labels{
			labelKeyHandlerName: message.HandlerNameFromCtx(ctx),
		}

		defer func() {
			if err != nil {
				labels[labelSuccess] = "false"
			} else {
				labels[labelSuccess] = "true"
			}
			m.handlerExecutionTimeSeconds.With(labels).Observe(time.Since(now).Seconds())
		}()

		return h(msg)
	}
}

// NewRouterMiddleware returns new middleware.
func (b PrometheusMetricsBuilder) NewRouterMiddleware() HandlerPrometheusMetricsMiddleware {
	var err error
	m := HandlerPrometheusMetricsMiddleware{}

	if b.HandlerBuckets == nil {
		b.HandlerBuckets = defaultHandlerExecutionTimeBuckets
	}

	m.handlerExecutionTimeSeconds, err = b.registerHistogramVec(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "handler_execution_time_seconds",
			Help:      "The total time elapsed while executing the handler function in seconds",
			Buckets:   b.HandlerBuckets,
		},
		handlerLabelKeys,
	))
	if err != nil {
		panic(errors.Wrap(err, "could not register handler execution time metric"))
	}

	return m
}
