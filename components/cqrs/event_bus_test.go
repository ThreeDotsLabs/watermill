package cqrs_test

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventBus_Send_ContextPropagation(t *testing.T) {
	publisher := newPublisherStub()

	eventBus := cqrs.NewEventBus(
		publisher,
		func(eventName string) string {
			return "whatever"
		},
		cqrs.JSONMarshaler{},
	)

	ctx := context.WithValue(context.Background(), "key", "value")

	err := eventBus.Publish(ctx, "message")
	require.NoError(t, err)

	assert.Equal(t, ctx, publisher.messages["whatever"][0].Context())
}

func TestEventBus_Send_topic_name(t *testing.T) {
	cb := cqrs.NewEventBus(
		assertPublishTopicPublisher{"cqrs_test.TestEvent", t},
		func(commandName string) string {
			return commandName
		},
		cqrs.JSONMarshaler{},
	)

	err := cb.Publish(context.Background(), TestEvent{})
	require.NoError(t, err)
}
