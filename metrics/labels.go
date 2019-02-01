package metrics

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/prometheus/client_golang/prometheus"
)

type labelKey string

const (
	labelKeyHandlerName    = "handler_name"
	labelKeyPublisherName  = "publisher_name"
	labelKeySubscriberName = "subscriber_name"
	labelSuccess           = "success"
	labelAcked             = "acked"
)

var (
	labelGetters = map[labelKey]func(context.Context) string{
		labelKeyHandlerName:    message.HandlerNameFromCtx,
		labelKeyPublisherName:  message.PublisherNameFromCtx,
		labelKeySubscriberName: message.SubscriberNameFromCtx,
	}
	labelKeys []string
)

func init() {
	labelKeys = make([]string, len(labelGetters))
	i := 0
	for k := range labelGetters {
		labelKeys[i] = string(k)
		i++
	}
}

func labelsFromCtx(ctx context.Context, labels ...string) prometheus.Labels {
	ctxLabels := map[string]string{}

	for _, l := range labels {
		k := labelKey(l)
		getter, ok := labelGetters[k]
		if !ok {
			continue
		}

		v := getter(ctx)
		ctxLabels[l] = v
	}

	return ctxLabels
}
