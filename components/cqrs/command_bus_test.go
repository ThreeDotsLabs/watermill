package cqrs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

func TestCommandBus_Send_ContextPropagation(t *testing.T) {
	publisher := newPublisherStub()

	commandBus := cqrs.NewCommandBus(
		publisher,
		func(commandName string) string {
			return "whatever"
		},
		cqrs.JSONMarshaler{},
	)

	ctx := context.WithValue(context.Background(), "key", "value")

	err := commandBus.Send(ctx, "message")
	require.NoError(t, err)

	assert.Equal(t, ctx, publisher.messages["whatever"][0].Context())
}

func TestCommandBus_Send_topic_name(t *testing.T) {
	cb := cqrs.NewCommandBus(
		assertPublishTopicPublisher{"cqrs_test.TestCommand", t},
		func(commandName string) string {
			return commandName
		},
		cqrs.JSONMarshaler{},
	)

	err := cb.Send(context.Background(), TestCommand{})
	require.NoError(t, err)
}
