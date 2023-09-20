package requestreply_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/requestreply"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestServices[Response any] struct {
	Logger    watermill.LoggerAdapter
	Marshaler cqrs.CommandEventMarshaler
	PubSub    *gochannel.GoChannel
	Router    *message.Router

	CommandBus       *cqrs.CommandBus
	CommandProcessor *cqrs.CommandProcessor

	RequestReplyBackend *requestreply.PubSubRequestReply[Response]
}

func NewTestServices[Response any](t *testing.T) TestServices[Response] {
	t.Helper()

	logger := watermill.NewStdLogger(true, true)
	marshaler := cqrs.JSONMarshaler{}

	pubSub := gochannel.NewGoChannel(
		gochannel.Config{BlockPublishUntilSubscriberAck: false},
		logger,
	)

	backend, err := requestreply.NewPubSubRequestReply[Response](
		requestreply.PubSubRequestReplyConfig{
			Publisher: pubSub,
			SubscriberConstructor: func(subscriberContext requestreply.PubSubRequestReplySubscriberContext) (message.Subscriber, error) {
				return pubSub, nil
			},
			GenerateReplyNotificationTopic: func(subscriberContext requestreply.PubSubRequestReplySubscriberContext) (string, error) {
				return "reply", nil
			},
			Logger: logger,
			ModifyNotificationMessage: func(msg *message.Message, context requestreply.PubSubRequestReplyOnCommandProcessedContext) error {
				// to make it deterministic
				msg.UUID = "1"
				return nil
			},
		},
		requestreply.JsonMarshaler[Response]{},
	)
	require.NoError(t, err)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	require.NoError(t, err)

	commandBus, err := cqrs.NewCommandBusWithConfig(pubSub, cqrs.CommandBusConfig{
		GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
			return "commands", nil
		},
		Marshaler: marshaler,
		Logger:    logger,
	})
	require.NoError(t, err)

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(router, cqrs.CommandProcessorConfig{
		GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
			return "commands", nil
		},
		SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
			return pubSub, nil
		},
		Marshaler:                marshaler,
		Logger:                   logger,
		AckCommandHandlingErrors: true,
	})
	require.NoError(t, err)

	return TestServices[Response]{
		Logger: logger,
		PubSub: gochannel.NewGoChannel(
			gochannel.Config{BlockPublishUntilSubscriberAck: true},
			logger,
		),
		Router:              router,
		RequestReplyBackend: backend,
		CommandBus:          commandBus,
		CommandProcessor:    commandProcessor,
		Marshaler:           marshaler,
	}
}

func (ts TestServices[Response]) RunRouter() {
	go func() {
		err := ts.Router.Run(context.Background())
		if err != nil {
			panic(err)
		}
	}()

	<-ts.Router.Running()
}

type TestCommand struct {
	ID string `json:"id"`
}

type TestCommandWithResponse struct {
	ID string `json:"id"`
}

type TestCommandResponse struct {
	ID string `json:"id"`
}

func TestRequestReply_without_response_no_error(t *testing.T) {
	ts := NewTestServices[struct{}](t)

	err := ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandler(
			"test_handler",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand) error {
				return nil
			},
		),
	)
	require.NoError(t, err)

	ts.RunRouter()

	replyCh, err := requestreply.SendAndWait[struct{}](
		context.Background(),
		ts.CommandBus,
		ts.RequestReplyBackend,
		&TestCommand{ID: "1"},
	)
	require.NoError(t, err)
	require.NotNil(t, replyCh)

	select {
	case reply := <-replyCh:
		assert.Empty(t, reply.HandlerResponse)
		assert.NoError(t, reply.HandlerErr)
		assert.NotEmpty(t, reply.ReplyMsg.Metadata.Get(requestreply.NotificationIdMetadataKey))
		//assert.EqualValues(t, tc.ExpectedReply.ReplyMsg.Payload, reply.ReplyMsg.Payload) todo?
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_without_response_with_error(t *testing.T) {
	ts := NewTestServices[struct{}](t)

	expectedErr := errors.New("some error")

	err := ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandler(
			"test_handler",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand) error {
				return expectedErr
			},
		),
	)
	require.NoError(t, err)

	ts.RunRouter()

	replyCh, err := requestreply.SendAndWait[struct{}](
		context.Background(),
		ts.CommandBus,
		ts.RequestReplyBackend,
		&TestCommand{ID: "1"},
	)
	require.NoError(t, err)
	require.NotNil(t, replyCh)

	select {
	case reply := <-replyCh:
		assert.Empty(t, reply.HandlerResponse)

		require.Error(t, reply.HandlerErr)
		assert.Equal(t, expectedErr.Error(), reply.HandlerErr.Error())

		assert.NotEmpty(t, reply.ReplyMsg.Metadata.Get(requestreply.NotificationIdMetadataKey))
		//assert.EqualValues(t, tc.ExpectedReply.ReplyMsg.Payload, reply.ReplyMsg.Payload) todo?
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_with_response_no_error(t *testing.T) {
	ts := NewTestServices[TestCommandResponse](t)

	expectedResponse := TestCommandResponse{ID: "123"}

	err := ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandlerWithResponse[TestCommand, TestCommandResponse](
			"test_handler",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand) (TestCommandResponse, error) {
				return expectedResponse, nil
			},
		),
	)
	require.NoError(t, err)

	ts.RunRouter()

	replyCh, err := requestreply.SendAndWait[TestCommandResponse](
		context.Background(),
		ts.CommandBus,
		ts.RequestReplyBackend,
		&TestCommand{ID: "1"},
	)
	require.NoError(t, err)
	require.NotNil(t, replyCh)

	select {
	case reply := <-replyCh:
		assert.EqualValues(t, expectedResponse, reply.HandlerResponse)
		assert.NoError(t, reply.HandlerErr)
		assert.NotEmpty(t, reply.ReplyMsg.Metadata.Get(requestreply.NotificationIdMetadataKey))
		//assert.EqualValues(t, tc.ExpectedReply.ReplyMsg.Payload, reply.ReplyMsg.Payload) todo?
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_with_response_with_error(t *testing.T) {
	ts := NewTestServices[TestCommandResponse](t)

	expectedResponse := TestCommandResponse{ID: "123"}
	expectedErr := errors.New("some error")

	err := ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandlerWithResponse[TestCommand, TestCommandResponse](
			"test_handler",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand) (TestCommandResponse, error) {
				return expectedResponse, expectedErr
			},
		),
	)
	require.NoError(t, err)

	ts.RunRouter()

	replyCh, err := requestreply.SendAndWait[TestCommandResponse](
		context.Background(),
		ts.CommandBus,
		ts.RequestReplyBackend,
		&TestCommand{ID: "1"},
	)
	require.NoError(t, err)
	require.NotNil(t, replyCh)

	select {
	case reply := <-replyCh:
		assert.EqualValues(t, TestCommandResponse{ID: "123"}, reply.HandlerResponse)

		require.Error(t, reply.HandlerErr)
		assert.Equal(t, expectedErr.Error(), reply.HandlerErr.Error())

		assert.NotEmpty(t, reply.ReplyMsg.Metadata.Get(requestreply.NotificationIdMetadataKey))
		//assert.EqualValues(t, tc.ExpectedReply.ReplyMsg.Payload, reply.ReplyMsg.Payload) todo?
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

// todo; test multiple responses
// todo: test timeouts
// todo: assert metadatas
// todo: test ctx cancelation
// todo: test multiple handlers and messages in parallel
