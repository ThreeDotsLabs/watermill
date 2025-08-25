package metrics

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// SubscriberOpenTelemetryMetricsDecorator decorates a subscriber to capture OpenTelemetry metrics.
type SubscriberOpenTelemetryMetricsDecorator struct {
	message.Subscriber
	subscriberName                  string
	subscriberMessagesReceivedTotal metric.Int64Counter
}

func (s SubscriberOpenTelemetryMetricsDecorator) recordMetrics(msg *message.Message) {
	if msg == nil {
		return
	}

	ctx := msg.Context()
	labelsMap := labelsFromCtx(ctx, subscriberLabelKeys...)
	if labelsMap[labelKeySubscriberName] == "" {
		labelsMap[labelKeySubscriberName] = s.subscriberName
	}
	if labelsMap[labelKeyHandlerName] == "" {
		labelsMap[labelKeyHandlerName] = labelValueNoHandler
	}
	labels := make([]attribute.KeyValue, 0, len(labelsMap))
	for k, v := range labelsMap {
		labels = append(labels, attribute.String(k, v))
	}

	go func() {
		if subscribeAlreadyObserved(ctx) {
			// decorator idempotency when applied decorator multiple times
			return
		}

		select {
		case <-msg.Acked():
			labels = append(labels, attribute.String(labelAcked, "acked"))
		case <-msg.Nacked():
			labels = append(labels, attribute.String(labelAcked, "nacked"))
		}
		s.subscriberMessagesReceivedTotal.Add(ctx, 1, metric.WithAttributes(labels...))
	}()

	msg.SetContext(setSubscribeObservedToCtx(msg.Context()))
}
