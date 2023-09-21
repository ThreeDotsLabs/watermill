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

type MetricLabel struct {
	Label     string
	ComputeFn LabelComputeFn
}

func toLabelsSlice(baseLabels []string, customs []MetricLabel) []string {
	labels := make([]string, len(baseLabels), len(baseLabels)+len(customs))
	copy(labels, baseLabels)
	for _, label := range customs {
		//Check if the additional label is already in the base labels. We cannot have duplicate labels
		//If it's in the base, just skip it as the compute function is going to overwrite the default value
		contains := false
		for _, baseLabel := range baseLabels {
			if baseLabel == label.Label {
				contains = true
				break
			}
		}
		if !contains {
			labels = append(labels, label.Label)
		}
	}
	return labels
}
