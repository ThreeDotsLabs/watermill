package metrics

import (
	"time"

	"github.com/pkg/errors"

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

type PublisherPrometheusMetricsDecorator struct {
	pub                message.Publisher
	publishTimeSeconds *prometheus.HistogramVec
}

// Publish updates the relevant publisher metrics and calls the wrapped publisher's Publish.
func (m PublisherPrometheusMetricsDecorator) Publish(topic string, messages ...*message.Message) (err error) {
	if len(messages) == 0 {
		return m.pub.Publish(topic)
	}

	// TODO: take ctx not only from first msg. Might require changing the signature of Publish, which is planned anyway.
	ctx := messages[0].Context()
	labels := labelsFromCtx(ctx, publisherLabelKeys...)
	start := time.Now()

	defer func() {
		if err != nil {
			labels[labelSuccess] = "false"
		} else {
			labels[labelSuccess] = "true"
		}
		m.publishTimeSeconds.With(labels).Observe(time.Since(start).Seconds())
	}()
	return m.pub.Publish(topic, messages...)
}

// Close decreases the total publisher count, closes the Prometheus HTTP server and calls wrapped Close.
func (m PublisherPrometheusMetricsDecorator) Close() error {
	return m.pub.Close()
}

// DecoratePublisher wraps the underlying publisher with Prometheus metrics.
func (b PrometheusMetricsBuilder) DecoratePublisher(pub message.Publisher) (message.Publisher, error) {
	var err error
	d := PublisherPrometheusMetricsDecorator{
		pub: pub,
	}

	d.publishTimeSeconds, err = b.registerHistogramVec(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "publish_time_seconds",
			Help:      "The time that a publishing attempt (success or not) took in seconds",
		},
		publisherLabelKeys,
	))
	if err != nil {
		return nil, errors.Wrap(err, "could not register publish time metric")
	}
	return d, nil
}
