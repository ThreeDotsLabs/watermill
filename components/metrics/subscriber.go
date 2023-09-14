package metrics

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	subscriberLabelKeys = []string{
		labelKeyHandlerName,
		labelKeySubscriberName,
	}
)

// SubscriberPrometheusMetricsDecorator decorates a subscriber to capture Prometheus metrics.
type SubscriberPrometheusMetricsDecorator struct {
	message.Subscriber
	subscriberName                  string
	subscriberMessagesReceivedTotal *prometheus.CounterVec
	closing                         chan struct{}
	customLabels                    []metricLabel
}

func (s SubscriberPrometheusMetricsDecorator) recordMetrics(msg *message.Message) {
	if msg == nil {
		return
	}

	ctx := msg.Context()
	labels := labelsFromCtx(ctx, subscriberLabelKeys...)
	if labels[labelKeySubscriberName] == "" {
		labels[labelKeySubscriberName] = s.subscriberName
	}
	if labels[labelKeyHandlerName] == "" {
		labels[labelKeyHandlerName] = labelValueNoHandler
	}
	for _, customLabel := range s.customLabels {
		labels[customLabel.label] = customLabel.computeFn(ctx)
	}

	go func() {
		if subscribeAlreadyObserved(ctx) {
			// decorator idempotency when applied decorator multiple times
			return
		}

		select {
		case <-msg.Acked():
			labels[labelAcked] = "acked"
		case <-msg.Nacked():
			labels[labelAcked] = "nacked"
		}
		s.subscriberMessagesReceivedTotal.With(labels).Inc()
	}()

	msg.SetContext(setSubscribeObservedToCtx(msg.Context()))
}
