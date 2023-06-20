package cqrs_test

import (
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
)

func TestEventConfig_ValidateForProcessor(t *testing.T) {
	testCases := []struct {
		Name              string
		ModifyValidConfig func(*cqrs.EventConfig)
		ExpectedErr       error
	}{
		{
			Name:              "valid_config",
			ModifyValidConfig: nil,
			ExpectedErr:       nil,
		},
		{
			Name: "missing_GenerateHandlerSubscribeTopic",
			ModifyValidConfig: func(config *cqrs.EventConfig) {
				config.GenerateHandlerSubscribeTopic = nil
			},
			ExpectedErr: fmt.Errorf("missing GenerateHandlerTopic while SubscriberConstructor is provided"),
		},
		{
			Name: "valid_with_group_handlers",
			ModifyValidConfig: func(config *cqrs.EventConfig) {
				config.GenerateHandlerSubscribeTopic = nil
				config.SubscriberConstructor = nil

				config.GenerateHandlerGroupSubscribeTopic = func(params cqrs.GenerateEventHandlerGroupTopicParams) (string, error) {
					return "", nil
				}
				config.GroupSubscriberConstructor = func(params cqrs.EventsGroupSubscriberConstructorParams) (message.Subscriber, error) {
					return nil, nil
				}
			},
			ExpectedErr: nil,
		},
		{
			Name: "missing_GroupSubscriberConstructor",
			ModifyValidConfig: func(config *cqrs.EventConfig) {
				config.GenerateHandlerSubscribeTopic = nil
				config.SubscriberConstructor = nil

				config.GenerateHandlerGroupSubscribeTopic = func(params cqrs.GenerateEventHandlerGroupTopicParams) (string, error) {
					return "", nil
				}
			},
			ExpectedErr: fmt.Errorf("missing GroupSubscriberConstructor while GenerateHandlerGroupTopic is provided"),
		},
		{
			Name: "missing_GenerateHandlerGroupSubscribeTopic",
			ModifyValidConfig: func(config *cqrs.EventConfig) {
				config.GenerateHandlerSubscribeTopic = nil
				config.SubscriberConstructor = nil

				config.GroupSubscriberConstructor = func(params cqrs.EventsGroupSubscriberConstructorParams) (message.Subscriber, error) {
					return nil, nil
				}
			},
			ExpectedErr: fmt.Errorf("missing GenerateHandlerGroupTopic while GroupSubscriberConstructor is provided"),
		},
		{
			Name: "missing_marshaler",
			ModifyValidConfig: func(config *cqrs.EventConfig) {
				config.Marshaler = nil
			},
			ExpectedErr: fmt.Errorf("missing Marshaler"),
		},
		{
			Name: "missing_subscriber_constructor",
			ModifyValidConfig: func(config *cqrs.EventConfig) {
				config.SubscriberConstructor = nil
			},
			ExpectedErr: fmt.Errorf("missing SubscriberConstructor while GenerateHandlerTopic is provided"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := cqrs.EventConfig{
				GenerateHandlerSubscribeTopic: func(params cqrs.GenerateEventHandlerSubscribeTopicParams) (string, error) {
					return "", nil
				},
				SubscriberConstructor: func(params cqrs.EventsSubscriberConstructorParams) (message.Subscriber, error) {
					return nil, nil
				},
				Marshaler: cqrs.JSONMarshaler{},
			}

			if tc.ModifyValidConfig != nil {
				tc.ModifyValidConfig(&validConfig)
			}

			err := validConfig.ValidateForProcessor()
			if tc.ExpectedErr == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.ExpectedErr.Error())
			}
		})
	}
}

func TestEventConfig_ValidateForBus(t *testing.T) {
	testCases := []struct {
		Name              string
		ModifyValidConfig func(*cqrs.EventConfig)
		ExpectedErr       error
	}{
		{
			Name:              "valid_config",
			ModifyValidConfig: nil,
			ExpectedErr:       nil,
		},
		{
			Name: "missing_GenerateEventPublishTopic",
			ModifyValidConfig: func(config *cqrs.EventConfig) {
				config.GeneratePublishTopic = nil
			},
			ExpectedErr: fmt.Errorf("missing GenerateHandlerTopic"),
		},
		{
			Name: "missing_marshaler",
			ModifyValidConfig: func(config *cqrs.EventConfig) {
				config.Marshaler = nil
			},
			ExpectedErr: fmt.Errorf("missing Marshaler"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := cqrs.EventConfig{
				GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
					return "", nil
				},
				Marshaler: cqrs.JSONMarshaler{},
			}

			if tc.ModifyValidConfig != nil {
				tc.ModifyValidConfig(&validConfig)
			}

			err := validConfig.ValidateForBus()
			if tc.ExpectedErr == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.ExpectedErr.Error())
			}
		})
	}
}
