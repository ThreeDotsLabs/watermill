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
		getter, ok := labelGetters[k]
		if !ok {
			continue
		}

		v := getter(ctx)
		ctxLabels[l] = v
	}

	return ctxLabels
}
