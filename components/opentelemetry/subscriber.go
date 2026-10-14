package opentelemetry

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	subscriberLabelKeys = []string{
		labelKeyHandlerName,
		labelKeySubscriberName,
	}
)

// SubscriberOpenTelemetryMetricsDecorator decorates a subscriber to capture OpenTelemetry metrics.
type SubscriberOpenTelemetryMetricsDecorator struct {
	message.Subscriber
	subscriberName                  string
	subscriberMessagesReceivedTotal metric.Int64Counter

	closing   chan struct{}
	closeOnce sync.Once
}

// Close closes the decorator closing channel and calls the wrapped Close.
func (s *SubscriberOpenTelemetryMetricsDecorator) Close() error {
	s.closeOnce.Do(func() {
		close(s.closing)
	})
	return s.Subscriber.Close()
}

func (s *SubscriberOpenTelemetryMetricsDecorator) recordMetrics(msg *message.Message) {
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

	labels := make([]attribute.KeyValue, 0, len(labelsMap)+1)
	for k, v := range labelsMap {
		labels = append(labels, attribute.String(k, v))
	}

	metricCtx := context.WithoutCancel(ctx)

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
		case <-s.closing:
			return // avoid goroutine leak if subscriber is closed
		}
		s.subscriberMessagesReceivedTotal.Add(metricCtx, 1, metric.WithAttributes(labels...))
	}()

	msg.SetContext(setSubscribeObservedToCtx(msg.Context()))
}
