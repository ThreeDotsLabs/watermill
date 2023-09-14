package metrics

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	publisherLabelKeys = []string{
		labelKeyHandlerName,
		labelKeyPublisherName,
		labelSuccess,
	}
)

// PublisherPrometheusMetricsDecorator decorates a publisher to capture Prometheus metrics.
type PublisherPrometheusMetricsDecorator struct {
	pub                message.Publisher
	publisherName      string
	publishTimeSeconds *prometheus.HistogramVec
	customLabels       []metricLabel
}

// Publish updates the relevant publisher metrics and calls the wrapped publisher's Publish.
func (m PublisherPrometheusMetricsDecorator) Publish(topic string, messages ...*message.Message) (err error) {
	if len(messages) == 0 {
		return m.pub.Publish(topic)
	}

	// TODO: take ctx not only from first msg. Might require changing the signature of Publish, which is planned anyway.
	ctx := messages[0].Context()
	labels := labelsFromCtx(ctx, publisherLabelKeys...)
	if labels[labelKeyPublisherName] == "" {
		labels[labelKeyPublisherName] = m.publisherName
	}
	if labels[labelKeyHandlerName] == "" {
		labels[labelKeyHandlerName] = labelValueNoHandler
	}
	for _, customLabel := range m.customLabels {
		labels[customLabel.label] = customLabel.computeFn(ctx)
	}
	start := time.Now()

	defer func() {
		if publishAlreadyObserved(ctx) {
			// decorator idempotency when applied decorator multiple times
			return
		}

		if err != nil {
			labels[labelSuccess] = "false"
		} else {
			labels[labelSuccess] = "true"
		}
		m.publishTimeSeconds.With(labels).Observe(time.Since(start).Seconds())
	}()

	for _, msg := range messages {
		msg.SetContext(setPublishObservedToCtx(msg.Context()))
	}

	return m.pub.Publish(topic, messages...)
}

// Close decreases the total publisher count, closes the Prometheus HTTP server and calls wrapped Close.
func (m PublisherPrometheusMetricsDecorator) Close() error {
	return m.pub.Close()
}
