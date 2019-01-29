package metrics

import (
	"time"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
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

	subscriberReceivedTotal    *prometheus.CounterVec
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

	s.subscriberReceivedTotal.With(labels).Inc()

	go func() {
		select {
		case <-msg.Acked():
			s.subscriberTimeToAckSeconds.With(labels).Observe(time.Since(now).Seconds())
		case <-s.closing:
			return
		}
	}()
}

// DecorateSubscriber wraps the underlying subscriber with Prometheus metrics.
func (b PrometheusMetricsBuilder) DecorateSubscriber(sub message.Subscriber) (message.Subscriber, error) {
	subscriberReceivedTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_received_total",
			Help:      "Total number of received messages",
		},
		subscriberLabelKeys,
	)

	subscriberTimeToAckSeconds := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_time_to_ack_seconds",
			Help:      "The time elapsed between obtaining a message and receiving an ACK",
		},
		subscriberLabelKeys,
	)

	subscriberCountTotal := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_count_total",
			Help:      "The total count of active subscribers",
		},
	)

	var err error
	for _, c := range []prometheus.Collector{
		subscriberReceivedTotal,
		subscriberTimeToAckSeconds,
		// subscriberCountTotal is WIP, don't register yet
	} {
		if registerErr := b.PrometheusRegistry.Register(c); registerErr != nil {
			err = multierror.Append(err, registerErr)
		}
	}
	if err != nil {
		return nil, err
	}

	wrapped := &SubscriberPrometheusMetricsDecorator{
		subscriberReceivedTotal:    subscriberReceivedTotal,
		subscriberTimeToAckSeconds: subscriberTimeToAckSeconds,
		subscriberCountTotal:       subscriberCountTotal,

		closing: make(chan struct{}),
	}

	wrapped.Subscriber, err = message.MessageTransformSubscriberDecorator(wrapped.recordMetrics, func(error) { close(wrapped.closing) })(sub)
	if err != nil {
		return nil, errors.Wrap(err, "could not decorate subscriber with metrics decorator")
	}

	subscriberCountTotal.Inc()
	return wrapped, nil
}
