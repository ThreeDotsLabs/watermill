package opentelemetry_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/opentelemetry"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

func TestOpenTelemetryMetrics(t *testing.T) {
	// Initialize ManualReader to retrieve metrics
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() {
		_ = provider.Shutdown(context.Background())
	}()

	meter := provider.Meter("watermill-test")
	builder := opentelemetry.NewOpenTelemetryMetricsBuilder(meter, "test_ns", "test_sub")

	pubSub := gochannel.NewGoChannel(gochannel.Config{}, nil)
	defer pubSub.Close()

	decoratedPub, err := builder.DecoratePublisher(pubSub)
	require.NoError(t, err)

	decoratedSub, err := builder.DecorateSubscriber(pubSub)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(true, false)
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)
	defer router.Close()

	builder.AddOpenTelemetryRouterMetrics(router)

	handledTopic := "test-topic"
	publishTopic := "published-topic"

	handlerCalled := make(chan struct{})
	router.AddHandler(
		"test_handler",
		handledTopic,
		decoratedSub,
		publishTopic,
		decoratedPub,
		func(msg *message.Message) ([]*message.Message, error) {
			close(handlerCalled)
			return []*message.Message{message.NewMessage("reply-uuid", []byte("reply"))}, nil
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = router.Run(ctx)
	}()

	// Wait for router to be running
	select {
	case <-router.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("router did not start")
	}

	msg := message.NewMessage("msg-uuid", []byte("payload"))
	err = decoratedPub.Publish(handledTopic, msg)
	require.NoError(t, err)

	// Wait for handler execution
	select {
	case <-handlerCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called")
	}

	// Give the router time to complete Ack and the background metrics recording goroutine to run
	time.Sleep(100 * time.Millisecond)

	// Read collected metrics
	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	metricsFound := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metricsFound[m.Name] = true
		}
	}

	assert.True(t, metricsFound["test_ns_test_sub_publish_time_seconds"], "publish_time_seconds metric not found")
	assert.True(t, metricsFound["test_ns_test_sub_subscriber_messages_received_total"], "subscriber_messages_received_total metric not found")
	assert.True(t, metricsFound["test_ns_test_sub_handler_execution_time_seconds"], "handler_execution_time_seconds metric not found")
}
