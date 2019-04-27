package cqrs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

func TestCommandBus_Send_topic_name(t *testing.T) {
	cb := cqrs.NewCommandBus(
		assertPublishTopicPublisher{"cqrs_test.TestCommand", t},
		func(commandName string) string {
			return commandName
		},
		cqrs.JSONMarshaler{},
	)

	err := cb.Send(TestCommand{})
	require.NoError(t, err)
}
