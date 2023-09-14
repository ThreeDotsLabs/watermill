package metrics

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	labelKeyHandlerName    = "handler_name"
	labelKeyPublisherName  = "publisher_name"
	labelKeySubscriberName = "subscriber_name"
	labelSuccess           = "success"
	labelAcked             = "acked"

	labelValueNoHandler = "<no handler>"
)

var (
	labelGetters = map[string]func(context.Context) string{
		labelKeyHandlerName:    message.HandlerNameFromCtx,
		labelKeyPublisherName:  message.PublisherNameFromCtx,
		labelKeySubscriberName: message.SubscriberNameFromCtx,
	}
)

func labelsFromCtx(ctx context.Context, labels ...string) prometheus.Labels {
	ctxLabels := map[string]string{}

	for _, l := range labels {
		k := l
		ctxLabels[l] = ""

		getter, ok := labelGetters[k]
		if !ok {
			continue
		}

		v := getter(ctx)
		if v != "" {
			ctxLabels[l] = v
		}
	}

	return ctxLabels
}

type LabelComputeFn func(msgCtx context.Context) string

type metricLabel struct {
	label     string
	computeFn LabelComputeFn
}

func appendCustomLabels(labels []string, customs []metricLabel) []string {
	for _, label := range customs {
		labels = append(labels, label.label)
	}
	return labels
}
