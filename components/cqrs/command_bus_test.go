package cqrs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

func TestNewCommandBus(t *testing.T) {
	pub := newPublisherStub()
	generateTopic := func(commandName string) string {
		return ""
	}
	marshaler := cqrs.JSONMarshaler{}

	cb, err := cqrs.NewCommandBus(pub, generateTopic, marshaler)
	assert.NotNil(t, cb)
	assert.NoError(t, err)

	cb, err = cqrs.NewCommandBus(nil, generateTopic, marshaler)
	assert.Nil(t, cb)
	assert.Error(t, err)

	cb, err = cqrs.NewCommandBus(pub, nil, marshaler)
	assert.Nil(t, cb)
	assert.Error(t, err)

	cb, err = cqrs.NewCommandBus(pub, generateTopic, nil)
	assert.Nil(t, cb)
	assert.Error(t, err)
}

func TestCommandBus_Send_ContextPropagation(t *testing.T) {
	publisher := newPublisherStub()

	commandBus, err := cqrs.NewCommandBus(
		publisher,
		func(commandName string) string {
			return "whatever"
		},
		cqrs.JSONMarshaler{},
	)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), "key", "value")

	err = commandBus.Send(ctx, "message")
	require.NoError(t, err)

	assert.Equal(t, ctx, publisher.messages["whatever"][0].Context())
}

func TestCommandBus_Send_topic_name(t *testing.T) {
	cb, err := cqrs.NewCommandBus(
		assertPublishTopicPublisher{ExpectedTopic: "cqrs_test.TestCommand", T: t},
		func(commandName string) string {
			return commandName
		},
		cqrs.JSONMarshaler{},
	)
	require.NoError(t, err)

	err = cb.Send(context.Background(), TestCommand{})
	require.NoError(t, err)
}
