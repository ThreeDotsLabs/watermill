package requestreply_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/requestreply"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replyTransport's channel is unbuffered, so Publish returns only after the listener took the message:
// the tests know when the reply buffer is full without sleeping.
type replyTransport struct {
	messages chan *message.Message
}

func newReplyTransport() replyTransport {
	return replyTransport{messages: make(chan *message.Message)}
}

func (r replyTransport) Publish(topic string, messages ...*message.Message) error {
	for _, msg := range messages {
		select {
		case r.messages <- msg:
		case <-time.After(time.Second):
			return errors.New("reply was not received by the listener")
		}
	}

	return nil
}

func (r replyTransport) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	return r.messages, nil
}

func (r replyTransport) Close() error {
	return nil
}

func newReplyTransportBackend(
	t *testing.T,
	transport replyTransport,
) (*requestreply.PubSubBackend[TestCommandResult], <-chan struct{}) {
	t.Helper()

	listenerFinished := make(chan struct{})

	backend, err := requestreply.NewPubSubBackend[TestCommandResult](
		requestreply.PubSubBackendConfig{
			Publisher: transport,
			SubscriberConstructor: func(params requestreply.PubSubBackendSubscribeParams) (message.Subscriber, error) {
				return transport, nil
			},
			GenerateSubscribeTopic: func(params requestreply.PubSubBackendSubscribeParams) (string, error) {
				return "reply", nil
			},
			GeneratePublishTopic: func(params requestreply.PubSubBackendPublishParams) (string, error) {
				return "reply", nil
			},
			OnListenForReplyFinished: func(ctx context.Context, params requestreply.PubSubBackendSubscribeParams) {
				close(listenerFinished)
			},
		},
		requestreply.BackendPubsubJSONMarshaler[TestCommandResult]{},
	)
	require.NoError(t, err)

	return backend, listenerFinished
}

func newReplyMessage(operationID requestreply.OperationID, payload string) *message.Message {
	msg := message.NewMessage(watermill.NewUUID(), []byte(payload))
	msg.Metadata.Set(requestreply.OperationIDMetadataKey, string(operationID))

	return msg
}

func requireListenerFinished(t *testing.T, listenerFinished <-chan struct{}) {
	t.Helper()

	select {
	case <-listenerFinished:
	case <-time.After(time.Second):
		t.Fatal("listener goroutine did not finish: OnListenForReplyFinished was not called")
	}
}

// fanOutCommandBus publishes all replies before SendWithModifiedMessage returns, so more replies
// arrive than SendWithReply reads: the case that leaked the listener goroutine.
type fanOutCommandBus struct {
	backend       *requestreply.PubSubBackend[TestCommandResult]
	handlersCount int
}

func (c fanOutCommandBus) SendWithModifiedMessage(ctx context.Context, cmd any, modify func(*message.Message) error) error {
	commandMsg := message.NewMessage(watermill.NewUUID(), nil)
	if err := modify(commandMsg); err != nil {
		return err
	}

	for i := 1; i <= c.handlersCount; i++ {
		err := c.backend.OnCommandProcessed(ctx, requestreply.BackendOnCommandProcessedParams[TestCommandResult]{
			Command:        cmd,
			CommandMessage: commandMsg,
			HandlerResult:  TestCommandResult{ID: fmt.Sprint(i)},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func TestSendWithReply_more_replies_than_read(t *testing.T) {
	transport := newReplyTransport()
	backend, listenerFinished := newReplyTransportBackend(t, transport)

	reply, err := requestreply.SendWithReply[TestCommandResult](
		context.Background(),
		fanOutCommandBus{backend: backend, handlersCount: 2},
		backend,
		&TestCommand{ID: "1"},
	)
	require.NoError(t, err)
	assert.Equal(t, TestCommandResult{ID: "1"}, reply.HandlerResult)

	// The second reply was not read by SendWithReply, it must not block the listener.
	requireListenerFinished(t, listenerFinished)
}

func TestPubSubBackend_ListenForNotifications_reader_stopped_reading(t *testing.T) {
	const operationID = requestreply.OperationID("operation-1")

	testCases := []struct {
		Name              string
		AfterBufferFilled func(t *testing.T, transport replyTransport)
	}{
		{
			Name:              "no_more_messages",
			AfterBufferFilled: func(t *testing.T, transport replyTransport) {},
		},
		{
			Name: "next_reply",
			AfterBufferFilled: func(t *testing.T, transport replyTransport) {
				require.NoError(t, transport.Publish("reply", newReplyMessage(operationID, `{"id":"2"}`)))
			},
		},
		{
			Name: "reply_with_unmarshal_error",
			AfterBufferFilled: func(t *testing.T, transport replyTransport) {
				require.NoError(t, transport.Publish("reply", newReplyMessage(operationID, "not json")))
			},
		},
		{
			Name: "subscriber_closed",
			AfterBufferFilled: func(t *testing.T, transport replyTransport) {
				close(transport.messages)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			transport := newReplyTransport()
			backend, listenerFinished := newReplyTransportBackend(t, transport)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			replyCh, err := backend.ListenForNotifications(ctx, requestreply.BackendListenForNotificationsParams{
				Command:     &TestCommand{ID: "1"},
				OperationID: operationID,
			})
			require.NoError(t, err)

			// The reply channel has a buffer of one reply: this reply fills it.
			require.NoError(t, transport.Publish("reply", newReplyMessage(operationID, `{"id":"1"}`)))

			tc.AfterBufferFilled(t, transport)

			cancel()
			requireListenerFinished(t, listenerFinished)

			// A reader coming back after the cancellation gets the reply that was already buffered
			// and then the closed channel. Replies and errors that did not fit into the buffer are dropped.
			reply, ok := <-replyCh
			require.True(t, ok, "the buffered reply should be still delivered")
			assert.Equal(t, TestCommandResult{ID: "1"}, reply.HandlerResult)
			assert.NoError(t, reply.Error)

			_, ok = <-replyCh
			assert.False(t, ok, "reply channel should be closed")
		})
	}
}

func TestPubSubBackend_ListenForNotifications_subscriber_closed_while_reply_buffered(t *testing.T) {
	const operationID = requestreply.OperationID("operation-1")

	transport := newReplyTransport()
	backend, listenerFinished := newReplyTransportBackend(t, transport)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	replyCh, err := backend.ListenForNotifications(ctx, requestreply.BackendListenForNotificationsParams{
		Command:     &TestCommand{ID: "1"},
		OperationID: operationID,
	})
	require.NoError(t, err)

	require.NoError(t, transport.Publish("reply", newReplyMessage(operationID, `{"id":"1"}`)))
	close(transport.messages)

	// The context is not done, so a reader that is still reading gets the error after the buffered reply.
	reply := <-replyCh
	assert.Equal(t, TestCommandResult{ID: "1"}, reply.HandlerResult)
	assert.NoError(t, reply.Error)

	select {
	case reply := <-replyCh:
		var replyTimeoutErr requestreply.ReplyTimeoutError
		require.ErrorAs(t, reply.Error, &replyTimeoutErr)
		assert.EqualError(t, replyTimeoutErr.Err, "subscriber closed")
	case <-time.After(time.Second):
		t.Fatal("subscriber closed error was not delivered")
	}

	requireListenerFinished(t, listenerFinished)

	_, ok := <-replyCh
	assert.False(t, ok, "reply channel should be closed")
}
