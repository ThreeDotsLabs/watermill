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
