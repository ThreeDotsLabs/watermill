package metrics

import (
	"github.com/ThreeDotsLabs/watermill/internal"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/metric"
)

func NewOpenTelemetryMetricsBuilder(meter metric.Meter, namespace string, subsystem string) OpenTelemetryMetricsBuilder {
	return OpenTelemetryMetricsBuilder{
		Namespace: namespace,
		Subsystem: subsystem,
		meter:     meter,
	}
}

// OpenTelemetryMetricsBuilder provides methods to decorate publishers, subscribers and handlers.
type OpenTelemetryMetricsBuilder struct {
	meter metric.Meter

	Namespace string
	Subsystem string
	// PublishBuckets defines the histogram buckets for publish time histogram, defaulted if nil.
	PublishBuckets []float64
	// HandlerBuckets defines the histogram buckets for handle execution time histogram, defaulted to watermill's default.
	HandlerBuckets []float64
}

// AddOpenTelemetryRouterMetrics is a convenience function that acts on the message router to add the metrics middleware
// to all its handlers. The handlers' publishers and subscribers are also decorated.
func (b OpenTelemetryMetricsBuilder) AddOpenTelemetryRouterMetrics(r *message.Router) {
	r.AddPublisherDecorators(b.DecoratePublisher)
	r.AddSubscriberDecorators(b.DecorateSubscriber)
	r.AddMiddleware(b.NewRouterMiddleware().Middleware)
}

// DecoratePublisher wraps the underlying publisher with OpenTelemetry metrics.
func (b OpenTelemetryMetricsBuilder) DecoratePublisher(pub message.Publisher) (message.Publisher, error) {
	var err error
	d := PublisherOpenTelemetryMetricsDecorator{
		pub:           pub,
		publisherName: internal.StructName(pub),
	}

	d.publishTimeSeconds, err = b.meter.Float64Histogram(
		b.name("publish_time_seconds"),
		metric.WithUnit("seconds"),
		metric.WithDescription("The time that a publishing attempt (success or not) took in seconds"),
		metric.WithExplicitBucketBoundaries(b.PublishBuckets...),
	)

	if err != nil {
		return nil, errors.Wrap(err, "could not register publish time metric")
	}
	return d, nil
}

// DecorateSubscriber wraps the underlying subscriber with OpenTelemetry metrics.
func (b OpenTelemetryMetricsBuilder) DecorateSubscriber(sub message.Subscriber) (message.Subscriber, error) {
	var err error
	d := &SubscriberOpenTelemetryMetricsDecorator{
		subscriberName: internal.StructName(sub),
	}

	d.subscriberMessagesReceivedTotal, err = b.meter.Int64Counter(
		b.name("subscriber_messages_received_total"),
		metric.WithDescription("The total number of messages received by the subscriber"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "could not register time to ack metric")
	}

	d.Subscriber, err = message.MessageTransformSubscriberDecorator(d.recordMetrics)(sub)
	if err != nil {
		return nil, errors.Wrap(err, "could not decorate subscriber with metrics decorator")
	}

	return d, nil
}
func (b OpenTelemetryMetricsBuilder) name(name string) string {
	if b.Subsystem != "" {
		name = b.Subsystem + "_" + name
	}
	if b.Namespace != "" {
		name = b.Namespace + "_" + name
	}
	return name
}
