package metrics

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// PublisherOpenTelemetryMetricsDecorator decorates a publisher to capture OpenTelemetry metrics.
type PublisherOpenTelemetryMetricsDecorator struct {
	pub                message.Publisher
	publisherName      string
	publishTimeSeconds metric.Float64Histogram
}

// Publish updates the relevant publisher metrics and calls the wrapped publisher's Publish.
func (m PublisherOpenTelemetryMetricsDecorator) Publish(topic string, messages ...*message.Message) (err error) {
	if len(messages) == 0 {
		return m.pub.Publish(topic)
	}

	// TODO: take ctx not only from first msg. Might require changing the signature of Publish, which is planned anyway.
	ctx := messages[0].Context()
	labelsMap := labelsFromCtx(ctx, publisherLabelKeys...)
	if labelsMap[labelKeyPublisherName] == "" {
		labelsMap[labelKeyPublisherName] = m.publisherName
	}
	if labelsMap[labelKeyHandlerName] == "" {
		labelsMap[labelKeyHandlerName] = labelValueNoHandler
	}
	labels := make([]attribute.KeyValue, len(labelsMap))
	for k, v := range labelsMap {
		labels = append(labels, attribute.String(k, v))
	}
	start := time.Now()

	defer func() {
		if publishAlreadyObserved(ctx) {
			// decorator idempotency when applied decorator multiple times
			return
		}

		if err != nil {
			labels = append(labels, attribute.String(labelSuccess, "false"))
		} else {
			labels = append(labels, attribute.String(labelSuccess, "true"))
		}

		m.publishTimeSeconds.Record(
			ctx,
			time.Since(start).Seconds(),
			metric.WithAttributes(labels...),
		)
	}()

	for _, msg := range messages {
		msg.SetContext(setPublishObservedToCtx(msg.Context()))
	}

	return m.pub.Publish(topic, messages...)
}

// Close decreases the total publisher count, closes the OpenTelemetry HTTP server and calls wrapped Close.
func (m PublisherOpenTelemetryMetricsDecorator) Close() error {
	return m.pub.Close()
}
