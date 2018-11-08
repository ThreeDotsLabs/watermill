package middleware_test

import (
	"testing"

	"github.com/roblaszczak/gooddd/message"

	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message/router/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const topic = "testing_poison_queue_topic"

// TestPoisonQueue_publisher_working_handler_ok simulates the situation when the message is processed correctly
// We expect that all messages pass through the middleware unaffected and the poison queue catches no messages.
func TestPoisonQueue_handler_ok(t *testing.T) {
	poisonPublisher := mockPublisher{behaviour: BehaviourAlwaysOK}
	poisonQueue, err := middleware.NewPoisonQueue(&poisonPublisher, topic)
	require.NoError(t, err)

	produced, err := poisonQueue.Middleware(handlerFuncAlwaysOK)(
		message.NewMessage("uuid", nil),
	)

	assert.NoError(t, err)
	assert.Equal(t, handlerFuncAlwaysOKMessages, produced)
	assert.Empty(t, poisonPublisher.PopMessages())
}

func TestPoisonQueue_handler_failing(t *testing.T) {
	poisonPublisher := mockPublisher{behaviour: BehaviourAlwaysOK}
	poisonQueue, err := middleware.NewPoisonQueue(&poisonPublisher, topic)
	require.NoError(t, err)

	msg := message.NewMessage("uuid", nil)
	produced, err := poisonQueue.Middleware(handlerFuncAlwaysFailing)(
		msg,
	)

	// the middleware itself should not fail; the publisher is working OK, so no error is passed down the chain
	assert.NoError(t, err)

	// but no messages should be passed
	assert.Empty(t, produced)

	// the original message should end up in the poison queue
	poisonMsgs := poisonPublisher.PopMessages()
	require.Len(t, poisonMsgs, 1)

	// todo: no idea how to check if proper payload is passed; see mockConsumedMessage.UnmarshalPayload to see why
	// there should be additional metadata telling why the message was poisoned
	// it should be the error that the handler failed with
	assert.Equal(t, errFailed.Error(), poisonMsgs[0].Metadata.Get(middleware.ReasonForPoisonedKey))
}

func TestPoisonQueue_handler_failing_publisher_failing(t *testing.T) {
	poisonPublisher := mockPublisher{behaviour: BehaviourAlwaysFail}

	poisonQueue, err := middleware.NewPoisonQueue(&poisonPublisher, topic)
	require.NoError(t, err)

	msg := message.NewMessage("uuid", nil)
	produced, err := poisonQueue.Middleware(handlerFuncAlwaysFailing)(
		msg,
	)

	// publisher failed, can't hide the error anymore
	assert.Equal(t, errFailed, errors.Cause(err))

	// can't really expect any produced messages
	assert.Empty(t, produced)

	// nor poison messages
	assert.Empty(t, poisonPublisher.PopMessages())
}

func TestPoisonQueue_handler_failing_publisher_panicking(t *testing.T) {
	poisonPublisher := mockPublisher{behaviour: BehaviourAlwaysPanic}

	poisonQueue, err := middleware.NewPoisonQueue(&poisonPublisher, topic)
	require.NoError(t, err)

	msg := message.NewMessage("uuid", nil)

	// if the publisher panics, we're in deep shit - better not catch that panic here
	assert.Panics(t, func() {
		poisonQueue.Middleware(handlerFuncAlwaysFailing)(
			msg,
		)
	})
}
