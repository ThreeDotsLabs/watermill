package metrics

import (
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
	subscriberMessagesReceivedTotal *prometheus.CounterVec
	closing                         chan struct{}
}

func (s SubscriberPrometheusMetricsDecorator) recordMetrics(msg *message.Message) {
	if msg == nil {
		return
	}

	ctx := msg.Context()
	labels := labelsFromCtx(ctx, subscriberLabelKeys...)

	go func() {
		select {
		case <-msg.Acked():
			labels[labelAcked] = "acked"
		case <-msg.Nacked():
			labels[labelAcked] = "nacked"
		}
		s.subscriberMessagesReceivedTotal.With(labels).Inc()
	}()
}

// DecorateSubscriber wraps the underlying subscriber with Prometheus metrics.
func (b PrometheusMetricsBuilder) DecorateSubscriber(sub message.Subscriber) (message.Subscriber, error) {
	var err error
	d := &SubscriberPrometheusMetricsDecorator{
		closing: make(chan struct{}),
	}

	d.subscriberMessagesReceivedTotal, err = b.registerCounterVec(prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: b.Namespace,
			Subsystem: b.Subsystem,
			Name:      "subscriber_messages_received_total",
			Help:      "The total number of messages received by the subscriber",
		},
		append(subscriberLabelKeys, labelAcked),
	))
	if err != nil {
		return nil, errors.Wrap(err, "could not register time to ack metric")
	}

	d.Subscriber, err = message.MessageTransformSubscriberDecorator(d.recordMetrics)(sub)
	if err != nil {
		return nil, errors.Wrap(err, "could not decorate subscriber with metrics decorator")
	}

	return d, nil
}
