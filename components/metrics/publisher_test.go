package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/metrics"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

type noOpPublisher struct{}

func (noOpPublisher) Publish(topic string, messages ...*message.Message) error { return nil }
func (noOpPublisher) Close() error                                             { return nil }

func TestPublisherPrometheusMetricsDecorator_publisher_name_in_consumer_handler(t *testing.T) {
	logger := watermill.NopLogger{}
	registry := prometheus.NewRegistry()
	builder := metrics.NewPrometheusMetricsBuilder(registry, "", "")

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)
	builder.AddPrometheusRouterMetrics(router)

	pub, err := builder.DecoratePublisher(noOpPublisher{})
	require.NoError(t, err)

	pubSub := gochannel.NewGoChannel(gochannel.Config{}, logger)
	published := make(chan struct{})

	router.AddConsumerHandler(
		"consumer-handler",
		"in",
		pubSub,
		func(msg *message.Message) error {
			out := message.NewMessage(watermill.NewUUID(), nil)
			out.SetContext(msg.Context())
			err := pub.Publish("out", out)
			close(published)
			return err
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = router.Run(ctx)
	}()
	<-router.Running()

	require.NoError(t, pubSub.Publish("in", message.NewMessage(watermill.NewUUID(), nil)))

	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("message was not processed")
	}
	require.NoError(t, router.Close())

	families, err := registry.Gather()
	require.NoError(t, err)

	var publisherNames []string
	for _, family := range families {
		if family.GetName() != "publish_time_seconds" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "publisher_name" {
					publisherNames = append(publisherNames, label.GetValue())
				}
			}
		}
	}

	require.Equal(t, []string{"metrics_test.noOpPublisher"}, publisherNames)
}
