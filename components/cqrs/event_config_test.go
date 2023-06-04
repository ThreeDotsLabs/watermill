package cqrs_test

import (
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
)

func TestEventConfig_Validate(t *testing.T) {
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
			Name: "missing_GenerateIndividualSubscriberTopic_and_GenerateHandlerGroupTopic",
			ModifyValidConfig: func(config *cqrs.EventConfig) {
				config.GenerateIndividualSubscriberTopic = nil
				config.GenerateHandlerGroupTopic = nil
			},
			ExpectedErr: fmt.Errorf("GenerateIndividualSubscriberTopic or GenerateHandlerGroupTopic must be set"),
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
			ExpectedErr: fmt.Errorf("missing SubscriberConstructor"),
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			validConfig := cqrs.EventConfig{
				GenerateIndividualSubscriberTopic: func(params cqrs.GenerateEventsTopicParams) string {
					return ""
				},
				GenerateHandlerGroupTopic: func(params cqrs.GenerateEventsGroupTopicParams) string {
					return ""
				},
				SubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
					return nil, nil
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
