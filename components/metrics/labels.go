package metrics

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
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

func labelsFromCtx(ctx context.Context, labels ...string) map[string]string {
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
