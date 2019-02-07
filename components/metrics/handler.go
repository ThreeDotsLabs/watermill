package metrics

import (
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	handlerLabelKeys = []string{
		labelKeyHandlerName,
		labelSuccess,
	}

	// handlerExecutionTimeBuckets are one order of magnitude smaller than default buckets (5ms~10s),
	// because the handler execution times are typically shorter (µs~ms range).
	handlerExecutionTimeBuckets = []float64{
		0.0005,
		0.001,
		0.0025,
		0.005,
		0.01,
		0.025,
		0.05,
		0.1,
		0.25,
		0.5,
		1,
	}
)

type HandlerPrometheusMetricsMiddleware struct {
	handlerExecutionTimeSeconds *prometheus.HistogramVec
}

func (m HandlerPrometheusMetricsMiddleware) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (msgs []*message.Message, err error) {
		now := time.Now()
		ctx := msg.Context()
		labels := labelsFromCtx(ctx, handlerLabelKeys...)

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

func (b PrometheusMetricsBuilder) NewRouterMiddleware() HandlerPrometheusMetricsMiddleware {
	var err error
	m := HandlerPrometheusMetricsMiddleware{}

	m.handlerExecutionTimeSeconds, err = b.registerHistogramVec(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "handler_execution_time_seconds",
			Help:      "The total time elapsed while executing the handler function in seconds",
			Buckets:   handlerExecutionTimeBuckets,
		},
		handlerLabelKeys,
	))
	if err != nil {
		panic(errors.Wrap(err, "could not register handler execution time metric"))
	}

	return m
}
