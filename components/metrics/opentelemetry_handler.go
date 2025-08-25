package metrics

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	handlerLabelKeys = []string{
		labelKeyHandlerName,
		labelSuccess,
	}

	// defaultHandlerExecutionTimeBuckets are one order of magnitude smaller than default buckets (5ms~10s),
	// because the handler execution times are typically shorter (µs~ms range).
	defaultHandlerExecutionTimeBuckets = []float64{
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

// HandlerOpenTelemetryMetricsMiddleware is a middleware that captures OpenTelemetry metrics.
type HandlerOpenTelemetryMetricsMiddleware struct {
	handlerExecutionTimeSeconds metric.Float64Histogram
}

// Middleware returns the middleware ready to be used with watermill's Router.
func (m HandlerOpenTelemetryMetricsMiddleware) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) (msgs []*message.Message, err error) {
		now := time.Now()
		ctx := msg.Context()
		labels := []attribute.KeyValue{
			attribute.String(labelKeyHandlerName, message.HandlerNameFromCtx(ctx)),
		}

		defer func() {
			if err != nil {
				labels = append(labels, attribute.String(labelSuccess, "false"))
			} else {
				labels = append(labels, attribute.String(labelSuccess, "true"))
			}
			m.handlerExecutionTimeSeconds.Record(
				ctx,
				time.Since(now).Seconds(),
				metric.WithAttributes(labels...),
			)
		}()

		return h(msg)
	}
}

// NewRouterMiddleware returns new middleware.
func (b OpenTelemetryMetricsBuilder) NewRouterMiddleware() HandlerOpenTelemetryMetricsMiddleware {
	var err error
	m := HandlerOpenTelemetryMetricsMiddleware{}

	if b.HandlerBuckets == nil {
		b.HandlerBuckets = defaultHandlerExecutionTimeBuckets
	}

	m.handlerExecutionTimeSeconds, err = b.meter.Float64Histogram(
		b.name("handler_execution_time_seconds"),
		metric.WithUnit("seconds"),
		metric.WithDescription("The total time elapsed while executing the handler function in seconds"),
		metric.WithExplicitBucketBoundaries(b.HandlerBuckets...),
	)
	if err != nil {
		panic(errors.Wrap(err, "could not register handler execution time metric"))
	}

	return m
}
