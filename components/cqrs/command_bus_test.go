package cqrs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
)

func TestCommandBusConfig_Validate(t *testing.T) {
	testCases := []struct {
		Name              string
		ModifyValidConfig func(*cqrs.CommandBusConfig)
		ExpectedErr       error
	}{
		{
			Name:              "valid_config",
			ModifyValidConfig: nil,
			ExpectedErr:       nil,
		},
		{
			Name: "missing_Marshaler",
			ModifyValidConfig: func(c *cqrs.CommandBusConfig) {
				c.Marshaler = nil
			},
			ExpectedErr: errors.New("missing Marshaler"),
		},
		{
			Name: "missing_GeneratePublishTopic",
			ModifyValidConfig: func(c *cqrs.CommandBusConfig) {
				c.GeneratePublishTopic = nil
			},
			ExpectedErr: errors.New("missing GeneratePublishTopic"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := cqrs.CommandBusConfig{
				GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
					return "", nil
				},
				Marshaler: cqrs.JSONMarshaler{},
			}

			if tc.ModifyValidConfig != nil {
				tc.ModifyValidConfig(&validConfig)
			}

			err := validConfig.Validate()
			if tc.ExpectedErr == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.ExpectedErr.Error())
			}
		})
	}
}

func TestNewCommandBus(t *testing.T) {
	pub := newPublisherStub()

	config := cqrs.CommandBusConfig{
		GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
			return "", nil
		},
		Marshaler: cqrs.JSONMarshaler{},
	}

	require.NoError(t, config.Validate())

	cb, err := cqrs.NewCommandBusWithConfig(pub, config)
	assert.NotNil(t, cb)
	assert.NoError(t, err)

	config.GeneratePublishTopic = nil
	require.Error(t, config.Validate())

	cb, err = cqrs.NewCommandBusWithConfig(pub, config)
	assert.Nil(t, cb)
	assert.Error(t, err)
}

type contextKey string

func TestCommandBus_Send_ContextPropagation(t *testing.T) {
	publisher := newPublisherStub()

	commandBus, err := cqrs.NewCommandBusWithConfig(
		publisher,
		cqrs.CommandBusConfig{
			GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
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
		cqrs.CommandBusConfig{
			GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
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
		cqrs.CommandBusConfig{
			GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
				return "whatever", nil
			},
			Marshaler: cqrs.JSONMarshaler{},
			OnSend: func(params cqrs.CommandBusOnSendParams) error {
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

func TestCommandBus_SendWithModifiedMessage(t *testing.T) {
	publisher := newPublisherStub()

	cb, err := cqrs.NewCommandBusWithConfig(
		publisher,
		cqrs.CommandBusConfig{
			GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
				return "whatever", nil
			},
			Marshaler: cqrs.JSONMarshaler{},
		},
	)
	require.NoError(t, err)

	err = cb.SendWithModifiedMessage(context.Background(), TestCommand{}, func(message *message.Message) error {
		message.Metadata.Set("key", "value")
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, "value", publisher.messages["whatever"][0].Metadata.Get("key"))
}

func TestCommandBus_SendWithModifiedMessage_modify_error(t *testing.T) {
	publisher := newPublisherStub()

	cb, err := cqrs.NewCommandBusWithConfig(
		publisher,
		cqrs.CommandBusConfig{
			GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
				return "whatever", nil
			},
			Marshaler: cqrs.JSONMarshaler{},
		},
	)
	require.NoError(t, err)

	expectedErr := errors.New("some error")

	err = cb.SendWithModifiedMessage(
		context.Background(),
		TestCommand{},
		func(message *message.Message) error {
			return expectedErr
		},
	)
	assert.ErrorContains(t, err, expectedErr.Error())
}

func TestCommandBus_Send_OnSend_error(t *testing.T) {
	publisher := newPublisherStub()

	expectedErr := errors.New("some error")

	cb, err := cqrs.NewCommandBusWithConfig(
		publisher,
		cqrs.CommandBusConfig{
			GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
				return "whatever", nil
			},
			Marshaler: cqrs.JSONMarshaler{},
			OnSend: func(params cqrs.CommandBusOnSendParams) error {
				return expectedErr
			},
		},
	)
	require.NoError(t, err)

	err = cb.Send(context.Background(), TestCommand{})
	require.EqualError(t, err, "cannot execute OnSend: some error")
}
