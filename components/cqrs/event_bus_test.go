package cqrs_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/stretchr/testify/require"
)

func TestEventBus_Send_topic_name(t *testing.T) {
	cb := cqrs.NewEventBus(
		assertPublishTopicPublisher{"cqrs_test.TestEvent", t},
		func(commandName string) string {
			return commandName
		},
		cqrs.JSONMarshaler{},
	)

	err := cb.Publish(TestEvent{})
	require.NoError(t, err)
}
