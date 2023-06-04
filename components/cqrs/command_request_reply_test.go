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

// TestRequestReply is a functional test for request-reply command handling.
func TestRequestReply_functional(t *testing.T) {
	ts := NewTestServices()

	requestReplyPubSub := gochannel.NewGoChannel(
		gochannel.Config{BlockPublishUntilSubscriberAck: false},
		ts.Logger,
	)

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
		ModifyNotificationMessage: func(msg *message.Message, context cqrs.PubSubRequestReplyOnCommandProcessedContext) error {
			// to make it deterministic
			msg.UUID = "1"
			return nil
		},
	})
	require.NoError(t, err)

	mockBackend := &RequestReplyBackendMock{
		Replies: []cqrs.CommandReply{
			{
				Err:      nil,
				ReplyMsg: message.NewMessage("1", []byte("foo")),
			},
		},
	}

	testCases := []struct {
		Name          string
		Backend       cqrs.RequestReplyBackend
		ExpectedReply cqrs.CommandReply
	}{
		{
			Name:          "mock",
			Backend:       mockBackend,
			ExpectedReply: mockBackend.Replies[0],
		},
		{
			Name:    "pubsub",
			Backend: pubSubBackend,
			ExpectedReply: cqrs.CommandReply{
				Err:      nil,
				ReplyMsg: message.NewMessage("1", []byte("{}")),
			},
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			commandBus := runCommandProcessorWithRequestReply(t, ts, tc.Backend)

			repliesCh, err := commandBus.SendAndWait(context.Background(), &TestCommand{})
			require.NoError(t, err)

			select {
			case reply := <-repliesCh:
				assert.EqualValues(t, tc.ExpectedReply.Err, reply.Err)
				assert.EqualValues(t, tc.ExpectedReply.ReplyMsg.UUID, reply.ReplyMsg.UUID)
				assert.EqualValues(t, tc.ExpectedReply.ReplyMsg.Payload, reply.ReplyMsg.Payload)
			case <-time.After(time.Second):
				t.Fatal("timeout")
			}
		})
	}
}

func TestRequestReply_modify_command_message_before_publish(t *testing.T) {
	ts := NewTestServices()

	replies := []cqrs.CommandReply{
		{
			Err:      fmt.Errorf("test error"),
			ReplyMsg: message.NewMessage("1", nil),
		},
	}
	mockBackend := &RequestReplyBackendMock{
		Replies: replies,
		CustomModifyCommandMessageBeforePublish: func(cmdMsg *message.Message, command any) error {
			assert.EqualValues(t, &TestCommand{}, command)
			cmdMsg.Metadata.Set("foo", "bar")
			return nil
		},
		CustomOnCommandProcessed: func(cmdMsg *message.Message, command any, handleErr error) error {
			assert.Equal(t, "bar", cmdMsg.Metadata.Get("foo"))
			return nil
		},
	}

	commandBus := runCommandProcessorWithRequestReply(t, ts, mockBackend)

	repliesCh, err := commandBus.SendAndWait(context.Background(), &TestCommand{})
	require.NoError(t, err)

	select {
	case reply := <-repliesCh:
		assert.EqualValues(t, replies[0], reply)

	case <-time.After(time.Second):
		t.Fatal("timeout on first reply")
	}
}

func TestRequestReply_on_command_processed(t *testing.T) {
	ts := NewTestServices()

	onCommandProcessedCalled := false

	expectedHandlerErr := fmt.Errorf("test error")

	replies := []cqrs.CommandReply{
		{
			Err:      expectedHandlerErr,
			ReplyMsg: message.NewMessage("1", nil),
		},
	}
	mockBackend := &RequestReplyBackendMock{
		Replies: replies,
		CustomOnCommandProcessed: func(cmdMsg *message.Message, command any, handleErr error) error {
			onCommandProcessedCalled = true
			require.NotEmpty(t, cmdMsg)
			assert.Equal(t, "cqrs_test.TestCommand", cmdMsg.Metadata.Get("name"))
			assert.EqualValues(t, &TestCommand{}, command)
			assert.Equal(t, expectedHandlerErr, handleErr)
			return nil
		},
	}

	commandBus := runCommandProcessorWithRequestReplyWithHandler(
		t,
		ts,
		mockBackend,
		func(ctx context.Context, cmd *TestCommand) error {
			return expectedHandlerErr
		},
	)

	repliesCh, err := commandBus.SendAndWait(context.Background(), &TestCommand{})
	require.NoError(t, err)

	select {
	case reply := <-repliesCh:
		assert.EqualValues(t, replies[0], reply)

	case <-time.After(time.Second):
		t.Fatal("timeout on first reply")
	}

	assert.True(t, onCommandProcessedCalled)
}

func TestRequestReply_reply_with_error(t *testing.T) {
	ts := NewTestServices()

	replies := []cqrs.CommandReply{
		{
			Err:      fmt.Errorf("test error"),
			ReplyMsg: message.NewMessage("1", nil),
		},
	}
	mockBackend := &RequestReplyBackendMock{
		Replies: replies,
	}

	commandBus := runCommandProcessorWithRequestReply(t, ts, mockBackend)

	repliesCh, err := commandBus.SendAndWait(context.Background(), &TestCommand{})
	require.NoError(t, err)

	select {
	case reply := <-repliesCh:
		assert.EqualValues(t, replies[0], reply)
	case <-time.After(time.Second):
		t.Fatal("timeout on first reply")
	}
}

func TestRequestReply_multiple_replies(t *testing.T) {
	ts := NewTestServices()

	replies := []cqrs.CommandReply{
		{
			Err:      nil,
			ReplyMsg: message.NewMessage("1", nil),
		},
		{
			Err:      nil,
			ReplyMsg: message.NewMessage("2", nil),
		},
	}
	mockBackend := &RequestReplyBackendMock{
		Replies: replies,
	}

	commandBus := runCommandProcessorWithRequestReply(t, ts, mockBackend)

	repliesCh, err := commandBus.SendAndWait(context.Background(), &TestCommand{})
	require.NoError(t, err)

	select {
	case reply := <-repliesCh:
		assert.EqualValues(t, replies[0], reply)
	case <-time.After(time.Second):
		t.Fatal("timeout on first reply")
	}

	select {
	case reply := <-repliesCh:
		assert.EqualValues(t, replies[1], reply)
	case <-time.After(time.Second):
		t.Fatal("timeout on second reply")
	}
}

func runCommandProcessorWithRequestReply(t *testing.T, ts TestServices, mockBackend cqrs.RequestReplyBackend) *cqrs.CommandBus {
	t.Helper()

	return runCommandProcessorWithRequestReplyWithHandler(
		t,
		ts,
		mockBackend,
		func(ctx context.Context, cmd *TestCommand) error {
			return nil
		},
	)
}

func runCommandProcessorWithRequestReplyWithHandler(
	t *testing.T,
	ts TestServices,
	mockBackend cqrs.RequestReplyBackend,
	handler func(ctx context.Context, cmd *TestCommand) error,
) *cqrs.CommandBus {
	t.Helper()

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	commandConfig := cqrs.CommandConfig{
		GenerateTopic: func(params cqrs.GenerateCommandsTopicParams) string {
			return "commands"
		},
		SubscriberConstructor: func(params cqrs.CommandsSubscriberConstructorParams) (message.Subscriber, error) {
			return ts.CommandsPubSub, nil
		},
		Marshaler:                ts.Marshaler,
		Logger:                   ts.Logger,
		RequestReplyEnabled:      true,
		RequestReplyBackend:      mockBackend,
		AckCommandHandlingErrors: true,
	}

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(commandConfig)
	require.NoError(t, err)

	commandProcessor.AddHandler(cqrs.NewCommandHandler(
		"command_handler",
		handler,
	))

	err = commandProcessor.AddHandlersToRouter(router)
	require.NoError(t, err)

	go func() {
		err = router.Run(context.Background())
		assert.NoError(t, err)
	}()

	<-router.Running()

	commandBus, err := cqrs.NewCommandBusWithConfig(ts.CommandsPubSub, commandConfig)
	require.NoError(t, err)

	return commandBus
}

type RequestReplyBackendMock struct {
	Replies []cqrs.CommandReply

	CustomModifyCommandMessageBeforePublish func(cmdMsg *message.Message, command any) error
	CustomOnCommandProcessed                func(cmdMsg *message.Message, command any, handleErr error) error
}

func (r RequestReplyBackendMock) ModifyCommandMessageBeforePublish(cmdMsg *message.Message, command any) error {
	if r.CustomModifyCommandMessageBeforePublish != nil {
		return r.CustomModifyCommandMessageBeforePublish(cmdMsg, command)
	}

	return nil
}

func (r RequestReplyBackendMock) ListenForReply(ctx context.Context, cmdMsg *message.Message, cmd any) (<-chan cqrs.CommandReply, error) {
	out := make(chan cqrs.CommandReply, len(r.Replies))

	for _, reply := range r.Replies {
		out <- reply
	}

	close(out)

	return out, nil
}

func (r RequestReplyBackendMock) OnCommandProcessed(cmdMsg *message.Message, cmd any, handleErr error) error {
	if r.CustomOnCommandProcessed != nil {
		return r.CustomOnCommandProcessed(cmdMsg, cmd, handleErr)
	}

	return nil
}
