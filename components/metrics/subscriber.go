package metrics

import (
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	subscriberLabelKeys = []string{
		labelKeyHandlerName,
		labelKeySubscriberName,
	}
)

type SubscriberPrometheusMetricsDecorator struct {
	message.Subscriber

	subscriberTimeToAckSeconds *prometheus.HistogramVec
	subscriberCountTotal       prometheus.Gauge

	closing chan struct{}
}

func (s SubscriberPrometheusMetricsDecorator) recordMetrics(msg *message.Message) {
	if msg == nil {
		return
	}

	now := time.Now()
	ctx := msg.Context()
	labels := labelsFromCtx(ctx, subscriberLabelKeys...)

	go func() {
		select {
		case <-msg.Acked():
			s.subscriberTimeToAckSeconds.With(labels).Observe(time.Since(now).Seconds())
		case <-s.closing:
			return
		}
	}()
}

func (s *SubscriberPrometheusMetricsDecorator) onClose(error) {
	close(s.closing)
}

// DecorateSubscriber wraps the underlying subscriber with Prometheus metrics.
func (b PrometheusMetricsBuilder) DecorateSubscriber(sub message.Subscriber) (message.Subscriber, error) {
	var err error
	d := &SubscriberPrometheusMetricsDecorator{
		closing: make(chan struct{}),
	}

	d.subscriberTimeToAckSeconds, err = b.registerHistogramVec(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_time_to_ack_seconds",
			Help:      "The time elapsed between obtaining a message and receiving an ACK",
		},
		subscriberLabelKeys,
	))
	if err != nil {
		return nil, errors.Wrap(err, "could not register time to ack metric")
	}

	// todo: unclear if decrementing the gauge when subscriber dies is trustworthy
	// don't register yet, WIP
	d.subscriberCountTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_count_total",
			Help:      "The total count of active subscribers",
		},
	)

	d.Subscriber, err = message.MessageTransformSubscriberDecorator(d.recordMetrics, d.onClose)(sub)
	if err != nil {
		return nil, errors.Wrap(err, "could not decorate subscriber with metrics decorator")
	}

	d.subscriberCountTotal.Inc()
	return d, nil
}
