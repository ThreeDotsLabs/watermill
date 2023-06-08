package cqrs

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestCommandConfig_Validate(t *testing.T) {
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
			Name: "missing_GenerateReplyNotificationTopic",
			ModifyValidConfig: func(c *CommandConfig) {
				c.GenerateBusTopic = nil
			},
			ExpectedErr: errors.Errorf("missing GenerateBusTopic"),
		},
		{
			Name: "missing_SubscriberConstructor",
			ModifyValidConfig: func(c *CommandConfig) {
				c.SubscriberConstructor = nil
			},
			ExpectedErr: errors.Errorf("missing SubscriberConstructor"),
		},
		{
			Name: "missing_Marshaler",
			ModifyValidConfig: func(c *CommandConfig) {
				c.Marshaler = nil
			},
			ExpectedErr: errors.Errorf("missing Marshaler"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := CommandConfig{
				GenerateBusTopic: func(params GenerateCommandBusTopicParams) (string, error) {
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

			err := validConfig.Validate()
			if tc.ExpectedErr == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.ExpectedErr.Error())
			}
		})
	}
}
