package cqrs

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestCommandConfig_ValidateForProcessor(t *testing.T) {
	testCases := []struct {
		Name              string
		ModifyValidConfig func(*CommandConfig)
		ExpectedErr       error
	}{
		{
			Name:              "valid_config",
			ModifyValidConfig: nil,
			ExpectedErr:       nil,
		},
		{
			Name: "missing_Marshaler",
			ModifyValidConfig: func(c *CommandConfig) {
				c.Marshaler = nil
			},
			ExpectedErr: errors.Errorf("missing Marshaler"),
		},
		{
			Name: "missing_SubscriberConstructor",
			ModifyValidConfig: func(c *CommandConfig) {
				c.SubscriberConstructor = nil
			},
			ExpectedErr: errors.Errorf("missing SubscriberConstructor"),
		},
		{
			Name: "missing_GenerateHandlerSubscribeTopic",
			ModifyValidConfig: func(c *CommandConfig) {
				c.GenerateHandlerSubscribeTopic = nil
			},
			ExpectedErr: errors.Errorf("missing GenerateHandlerSubscribeTopic"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := CommandConfig{
				GenerateHandlerSubscribeTopic: func(params GenerateCommandHandlerSubscribeTopicParams) (string, error) {
					return "", nil
				},
				SubscriberConstructor: func(params CommandsSubscriberConstructorParams) (message.Subscriber, error) {
					return nil, nil
				},
				Marshaler: JSONMarshaler{},
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

func TestCommandConfig_ValidateForBus(t *testing.T) {
	testCases := []struct {
		Name              string
		ModifyValidConfig func(*CommandConfig)
		ExpectedErr       error
	}{
		{
			Name:              "valid_config",
			ModifyValidConfig: nil,
			ExpectedErr:       nil,
		},
		{
			Name: "missing_Marshaler",
			ModifyValidConfig: func(c *CommandConfig) {
				c.Marshaler = nil
			},
			ExpectedErr: errors.Errorf("missing Marshaler"),
		},
		{
			Name: "missing_GeneratePublishTopic",
			ModifyValidConfig: func(c *CommandConfig) {
				c.GeneratePublishTopic = nil
			},
			ExpectedErr: errors.Errorf("missing GeneratePublishTopic"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := CommandConfig{
				GeneratePublishTopic: func(params GenerateCommandPublishTopicParams) (string, error) {
					return "", nil
				},
				Marshaler: JSONMarshaler{},
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
