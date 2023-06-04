package cqrs_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubSubRequestReply(t *testing.T) {
	ts := NewTestServices()

	requestReplyPubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		ts.Logger,
	)

	cmdMsg := message.NewMessage("1", []byte("foo"))
	command := &TestCommand{}

	handlerErr := fmt.Errorf("some error")

	onListenForReplyFinishedCalled := make(chan struct{})

	pubSubBackend, err := cqrs.NewPubSubRequestReply(cqrs.PubSubRequestReplyConfig{
		Publisher: requestReplyPubSub,
		SubscriberConstructor: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (message.Subscriber, error) {
			assert.EqualValues(t, command, subscriberContext.Command)
			assert.True(t, subscriberContext.CommandMessage.Equals(cmdMsg))

			return requestReplyPubSub, nil
		},
		GenerateReplyNotificationTopic: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (string, error) {
			assert.EqualValues(t, command, subscriberContext.Command)
			assert.True(t, subscriberContext.CommandMessage.Equals(cmdMsg))

			return "reply", nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    ts.Logger,
		ModifyNotificationMessage: func(msg *message.Message, context cqrs.PubSubRequestReplyOnCommandProcessedContext) error {
			// to make it deterministic
			msg.UUID = "1"
			return nil
		},
		OnListenForReplyFinished: func(ctx context.Context, subscriberContext cqrs.PubSubRequestReplySubscriberContext) {
			assert.EqualValues(t, command, subscriberContext.Command)
			assert.True(t, subscriberContext.CommandMessage.Equals(cmdMsg))

			close(onListenForReplyFinishedCalled)
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	err = pubSubBackend.ModifyCommandMessageBeforePublish(cmdMsg, command)
	require.NoError(t, err)

	repliesCh, err := pubSubBackend.ListenForReply(
		ctx,
		cmdMsg,
		command,
	)
	require.NoError(t, err)

	err = pubSubBackend.ModifyCommandMessageBeforePublish(cmdMsg, command)
	require.NoError(t, err)

	err = pubSubBackend.OnCommandProcessed(cmdMsg, command, handlerErr)
	require.NoError(t, err)

	select {
	case reply := <-repliesCh:
		assert.EqualValues(t, "1", reply.ReplyMsg.UUID)
		assert.EqualValues(
			t,
			`{"error":"some error","has_error":true}`,
			string(reply.ReplyMsg.Payload),
		)
		assert.EqualError(t, reply.Err, handlerErr.Error())
	case <-time.After(1 * time.Second):
		require.Fail(t, "timeout")
	}

	cancel()

	select {
	case <-onListenForReplyFinishedCalled:
		// ok
	case <-time.After(1 * time.Second):
		require.Fail(t, "timeout waiting for OnListenForReplyFinished")
	}
}

func TestPubSubRequestReply_timeout(t *testing.T) {
	ts := NewTestServices()

	requestReplyPubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		ts.Logger,
	)

	cmdMsg := message.NewMessage("1", []byte("foo"))
	command := &TestCommand{}

	timeout := time.Millisecond * 1

	pubSubBackend, err := cqrs.NewPubSubRequestReply(cqrs.PubSubRequestReplyConfig{
		Publisher: requestReplyPubSub,
		SubscriberConstructor: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (message.Subscriber, error) {
			return requestReplyPubSub, nil
		},
		GenerateReplyNotificationTopic: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (string, error) {
			return "reply", nil
		},
		Marshaler:             cqrs.JSONMarshaler{},
		Logger:                ts.Logger,
		ListenForReplyTimeout: &timeout,
	})
	require.NoError(t, err)

	err = pubSubBackend.ModifyCommandMessageBeforePublish(cmdMsg, command)
	require.NoError(t, err)

	repliesCh, err := pubSubBackend.ListenForReply(
		context.Background(),
		cmdMsg,
		command,
	)
	require.NoError(t, err)

	err = pubSubBackend.ModifyCommandMessageBeforePublish(cmdMsg, command)
	require.NoError(t, err)

	select {
	case reply := <-repliesCh:
		assert.ErrorContains(t, reply.Err, "reply timeout after")
		assert.ErrorContains(t, reply.Err, "context deadline exceeded")
	case <-time.After(1 * time.Second):
		require.Fail(t, "timeout")
	}
}

func TestPubSubRequestReply_timeout_context_cancelled(t *testing.T) {
	ts := NewTestServices()

	requestReplyPubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		ts.Logger,
	)

	cmdMsg := message.NewMessage("1", []byte("foo"))
	command := &TestCommand{}

	pubSubBackend, err := cqrs.NewPubSubRequestReply(cqrs.PubSubRequestReplyConfig{
		Publisher: requestReplyPubSub,
		SubscriberConstructor: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (message.Subscriber, error) {
			return requestReplyPubSub, nil
		},
		GenerateReplyNotificationTopic: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (string, error) {
			return "reply", nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    ts.Logger,
	})
	require.NoError(t, err)

	err = pubSubBackend.ModifyCommandMessageBeforePublish(cmdMsg, command)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	repliesCh, err := pubSubBackend.ListenForReply(
		ctx,
		cmdMsg,
		command,
	)
	require.NoError(t, err)

	err = pubSubBackend.ModifyCommandMessageBeforePublish(cmdMsg, command)
	require.NoError(t, err)

	cancel()

	select {
	case reply := <-repliesCh:
		assert.ErrorContains(t, reply.Err, "reply timeout after")
		assert.ErrorContains(t, reply.Err, "context canceled")
	case <-time.After(1 * time.Second):
		require.Fail(t, "timeout")
	}
}

func TestPubSubRequestReply_ListenForReply_unsupported_message(t *testing.T) {
	ts := NewTestServices()

	requestReplyPubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		ts.Logger,
	)

	cmdMsg := message.NewMessage("1", []byte("foo"))
	command := &TestCommand{}

	pubSubBackend, err := cqrs.NewPubSubRequestReply(cqrs.PubSubRequestReplyConfig{
		Publisher: requestReplyPubSub,
		SubscriberConstructor: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (message.Subscriber, error) {
			return requestReplyPubSub, nil
		},
		GenerateReplyNotificationTopic: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (string, error) {
			return "reply", nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    ts.Logger,
	})
	require.NoError(t, err)

	repliesCh, err := pubSubBackend.ListenForReply(
		context.Background(),
		cmdMsg,
		command,
	)
	assert.Empty(t, repliesCh)
	assert.EqualError(t, err, "RequestReply is enabled, but _watermill_notify_when_executed metadata is '' in command msg")
}

func TestPubSubRequestReply_unsupported_message_received(t *testing.T) {
	ts := NewTestServices()

	requestReplyPubSub := gochannel.NewGoChannel(
		gochannel.Config{},
		ts.Logger,
	)

	cmdMsg := message.NewMessage("1", []byte("foo"))
	command := &TestCommand{}

	pubSubBackend, err := cqrs.NewPubSubRequestReply(cqrs.PubSubRequestReplyConfig{
		Publisher: requestReplyPubSub,
		SubscriberConstructor: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (message.Subscriber, error) {
			return requestReplyPubSub, nil
		},
		GenerateReplyNotificationTopic: func(subscriberContext cqrs.PubSubRequestReplySubscriberContext) (string, error) {
			return "reply", nil
		},
		Marshaler: cqrs.JSONMarshaler{},
		Logger:    ts.Logger,
	})
	require.NoError(t, err)

	err = pubSubBackend.ModifyCommandMessageBeforePublish(cmdMsg, command)
	require.NoError(t, err)

	repliesCh, err := pubSubBackend.ListenForReply(
		context.Background(),
		cmdMsg,
		command,
	)
	require.NoError(t, err)

	// this msg has no _watermill_notify_when_executed metadata - should be ignored
	invalidCommandMsg := message.NewMessage("1", []byte("foo"))

	err = pubSubBackend.OnCommandProcessed(invalidCommandMsg, command, nil)
	require.NoError(t, err)

	select {
	case reply := <-repliesCh:
		t.Fatalf("no reply should be sent, but received %#v", reply)
	case <-time.After(time.Millisecond * 10):
		// ok
	}
}
