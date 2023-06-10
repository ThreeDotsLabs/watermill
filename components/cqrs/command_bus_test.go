package cqrs_test

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

func TestNewCommandBus(t *testing.T) {
	pub := newPublisherStub()

	commandConfig := cqrs.CommandConfig{
		GeneratePublishTopic: func(params cqrs.GenerateCommandPublishTopicParams) (string, error) {
			return "", nil
		},
		Marshaler: cqrs.JSONMarshaler{},
	}

	require.NoError(t, commandConfig.ValidateForBus())

	cb, err := cqrs.NewCommandBusWithConfig(pub, commandConfig)
	assert.NotNil(t, cb)
	assert.NoError(t, err)

	commandConfig.GeneratePublishTopic = nil
	require.Error(t, commandConfig.ValidateForBus())

	cb, err = cqrs.NewCommandBusWithConfig(pub, commandConfig)
	assert.Nil(t, cb)
	assert.Error(t, err)
}

type contextKey string

func TestCommandBus_Send_ContextPropagation(t *testing.T) {
	publisher := newPublisherStub()

	commandBus, err := cqrs.NewCommandBusWithConfig(
		publisher,
		cqrs.CommandConfig{
			GeneratePublishTopic: func(params cqrs.GenerateCommandPublishTopicParams) (string, error) {
				return "whatever", nil
			},
			Marshaler: cqrs.JSONMarshaler{},
		},
	)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), contextKey("key"), "value")

	err = commandBus.Send(ctx, "message")
	require.NoError(t, err)

	assert.Equal(t, ctx, publisher.messages["whatever"][0].Context())
}

func TestCommandBus_Send_topic_name(t *testing.T) {
	cb, err := cqrs.NewCommandBusWithConfig(
		assertPublishTopicPublisher{ExpectedTopic: "cqrs_test.TestCommand", T: t},
		cqrs.CommandConfig{
			GeneratePublishTopic: func(params cqrs.GenerateCommandPublishTopicParams) (string, error) {
				return params.CommandName, nil
			},
			Marshaler: cqrs.JSONMarshaler{},
		},
	)
	require.NoError(t, err)

	err = cb.Send(context.Background(), TestCommand{})
	require.NoError(t, err)
}

func TestCommandBus_Send_OnSend(t *testing.T) {
	publisher := newPublisherStub()

	cb, err := cqrs.NewCommandBusWithConfig(
		publisher,
		cqrs.CommandConfig{
			GeneratePublishTopic: func(params cqrs.GenerateCommandPublishTopicParams) (string, error) {
				return "whatever", nil
			},
			Marshaler: cqrs.JSONMarshaler{},
			OnSend: func(params cqrs.OnCommandSendParams) error {
				params.Message.Metadata.Set("key", "value")
				return nil
			},
		},
	)
	require.NoError(t, err)

	err = cb.Send(context.Background(), TestCommand{})
	require.NoError(t, err)

	assert.Equal(t, "value", publisher.messages["whatever"][0].Metadata.Get("key"))
}

func TestCommandBus_Send_OnSend_error(t *testing.T) {
	publisher := newPublisherStub()

	expectedErr := errors.New("some error")

	cb, err := cqrs.NewCommandBusWithConfig(
		publisher,
		cqrs.CommandConfig{
			GeneratePublishTopic: func(params cqrs.GenerateCommandPublishTopicParams) (string, error) {
				return "whatever", nil
			},
			Marshaler: cqrs.JSONMarshaler{},
			OnSend: func(params cqrs.OnCommandSendParams) error {
				return expectedErr
			},
		},
	)
	require.NoError(t, err)

	err = cb.Send(context.Background(), TestCommand{})
	require.EqualError(t, err, "cannot execute OnSend: some error")
}
