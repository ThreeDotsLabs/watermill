package middleware_test

import (
	"testing"

	"github.com/hashicorp/go-multierror"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const topic = "testing_poison_queue_topic"

// TestPoisonQueue_publisher_working_handler_ok simulates the situation when the message is processed correctly
// We expect that all messages pass through the middleware unaffected and the poison queue catches no messages.
func TestPoisonQueue_handler_ok(t *testing.T) {
	poisonPublisher := mockPublisher{behaviour: BehaviourAlwaysOK}

	poisonQueue, err := middleware.PoisonQueue(&poisonPublisher, topic)
	require.NoError(t, err)

	poisonQueueWithFilter, err := middleware.PoisonQueueWithFilter(&poisonPublisher, topic, func(err error) bool {
		return true
	})
	require.NoError(t, err)

	testCases := []struct {
		Name       string
		Middleware message.HandlerMiddleware
	}{
		{
			Name:       "PoisonQueue",
			Middleware: poisonQueue,
		},
		{
			Name:       "PoisonQueueWithFilter",
			Middleware: poisonQueueWithFilter,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			produced, err := c.Middleware(handlerFuncAlwaysOK)(
				message.NewMessage("uuid", nil),
			)

			assert.NoError(t, err)
			assert.Equal(t, handlerFuncAlwaysOKMessages, produced)
			assert.Empty(t, poisonPublisher.PopMessages())
		})
	}
}

func TestPoisonQueue_handler_failing(t *testing.T) {
	poisonPublisher := mockPublisher{behaviour: BehaviourAlwaysOK}

	poisonQueue, err := middleware.PoisonQueue(&poisonPublisher, topic)
	require.NoError(t, err)

	poisonQueueWithFilter, err := middleware.PoisonQueueWithFilter(&poisonPublisher, topic, func(err error) bool {
		return true
	})
	require.NoError(t, err)

	testCases := []struct {
		Name       string
		Middleware message.HandlerMiddleware
	}{
		{
			Name:       "PoisonQueue",
			Middleware: poisonQueue,
		},
		{
			Name:       "PoisonQueueWithFilter",
			Middleware: poisonQueueWithFilter,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			msg := message.NewMessage("uuid", []byte("payload"))
			produced, err := c.Middleware(handlerFuncAlwaysFailing)(
				msg,
			)

			// the middleware itself should not fail; the publisher is working OK, so no error is passed down the chain
			assert.NoError(t, err)

			// but no messages should be passed
			assert.Empty(t, produced)

			// the original message should end up in the poison queue
			poisonMsgs := poisonPublisher.PopMessages()
			require.Len(t, poisonMsgs, 1)

			assert.Equal(t, msg.Payload, poisonMsgs[0].Payload)

			// there should be additional metadata telling why the message was poisoned
			// it should be the error that the handler failed with
			assert.Equal(t, errFailed.Error(), poisonMsgs[0].Metadata.Get(middleware.ReasonForPoisonedKey))
		})
	}
}

func TestPoisonQueue_handler_failing_publisher_failing(t *testing.T) {
	poisonPublisher := mockPublisher{behaviour: BehaviourAlwaysFail}

	poisonQueue, err := middleware.PoisonQueue(&poisonPublisher, topic)
	require.NoError(t, err)

	poisonQueueWithFilter, err := middleware.PoisonQueueWithFilter(&poisonPublisher, topic, func(err error) bool {
		return true
	})
	require.NoError(t, err)

	testCases := []struct {
		Name       string
		Middleware message.HandlerMiddleware
	}{
		{
			Name:       "PoisonQueue",
			Middleware: poisonQueue,
		},
		{
			Name:       "PoisonQueueWithFilter",
			Middleware: poisonQueueWithFilter,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			msg := message.NewMessage("uuid", nil)
			produced, err := poisonQueue(handlerFuncAlwaysFailing)(
				msg,
			)

			require.IsType(t, &multierror.Error{}, err)
			multierr := err.(*multierror.Error)

			// publisher failed, can't hide the error anymore
			assert.Equal(t, errFailed, errors.Cause(multierr.WrappedErrors()[1]))

			// can't really expect any produced messages
			assert.Empty(t, produced)

			// nor poison messages
			assert.Empty(t, poisonPublisher.PopMessages())
		})
	}
}

func TestPoisonQueueWithFilter_poison_queue(t *testing.T) {
	poisonPublisher := mockPublisher{behaviour: BehaviourAlwaysOK}

	poisonQueueErr := errors.New("poison queue err")
	msg := message.NewMessage("uuid", []byte("payload"))

	poisonQueue, err := middleware.PoisonQueueWithFilter(&poisonPublisher, topic, func(err error) bool {
		return err == poisonQueueErr
	})
	require.NoError(t, err)

	_, err = poisonQueue(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, poisonQueueErr
	})(msg)

	assert.NoError(t, err)
	require.Len(t, poisonPublisher.PopMessages(), 1)
}

func TestPoisonQueueWithFilter_non_poison_queue(t *testing.T) {
	poisonPublisher := mockPublisher{behaviour: BehaviourAlwaysOK}

	nonPoisonQueueErr := errors.New("non poison queue err")
	msg := message.NewMessage("uuid", []byte("payload"))

	poisonQueue, err := middleware.PoisonQueueWithFilter(&poisonPublisher, topic, func(err error) bool {
		return err != nonPoisonQueueErr
	})
	require.NoError(t, err)

	_, err = poisonQueue(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, nonPoisonQueueErr
	})(msg)

	assert.Error(t, err)
	require.Len(t, poisonPublisher.PopMessages(), 0)
}
