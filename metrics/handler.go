package metrics

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	handlerLabelKeys = []string{
		labelKeyHandlerName,
	}
)

type HandlerPrometheusMetricsMiddleware struct {
	handlerSuccessesTotal       *prometheus.CounterVec
	handlerFailuresTotal        *prometheus.CounterVec
	handlerExecutionTimeSeconds *prometheus.HistogramVec
	handlerCountTotal           prometheus.Gauge
}

func (m HandlerPrometheusMetricsMiddleware) Middleware(h message.HandlerFunc) message.HandlerFunc {

	m.handlerCountTotal.Inc()

	return func(msg *message.Message) (msgs []*message.Message, err error) {
		now := time.Now()
		ctx := msg.Context()
		labels := labelsFromCtx(ctx, handlerLabelKeys...)

		defer func() {
			m.handlerExecutionTimeSeconds.With(labels).Observe(time.Since(now).Seconds())
			if err != nil {
				m.handlerFailuresTotal.With(labels).Inc()
				return
			}
			m.handlerSuccessesTotal.With(labels).Inc()
		}()

		return h(msg)
	}
}

func (b PrometheusMetricsBuilder) NewRouterMiddleware() HandlerPrometheusMetricsMiddleware {
	var err, registerErr error
	m := HandlerPrometheusMetricsMiddleware{}

	m.handlerSuccessesTotal, registerErr = b.registerCounterVec(prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "handler_successes_total",
			Help:      "The total number of times a handler succeeded",
		},
		handlerLabelKeys,
	))
	if registerErr != nil {
		err = multierror.Append(err, registerErr)
	}

	m.handlerFailuresTotal, registerErr = b.registerCounterVec(prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "handler_failures_total",
			Help:      "The total number of times a handler failed",
		},
		handlerLabelKeys,
	))
	if registerErr != nil {
		err = multierror.Append(err, registerErr)
	}

	m.handlerExecutionTimeSeconds, registerErr = b.registerHistogramVec(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "handler_execution_time_seconds",
			Help:      "The total time elapsed while executing the handler function in seconds",
		},
		handlerLabelKeys,
	))
	if registerErr != nil {
		err = multierror.Append(err, registerErr)
	}

	// todo: unclear how to decrement the gauge when handler dies
	// don't register yet, WIP
	m.handlerCountTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "handler_count_total",
			Help:      "The total count of active handlers",
		},
	)

	if err != nil {
		panic(err)
	}

	return m
}
