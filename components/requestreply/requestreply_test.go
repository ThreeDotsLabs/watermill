package requestreply_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/components/requestreply"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/google/uuid"
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

	RequestReplyBackend *requestreply.PubSubBackend[Response]
}

type TestServicesConfig struct {
	DoNotAckOnCommandErrors bool
	ListenForReplyTimeout   *time.Duration

	AssertNotificationMessage func(t *testing.T, msg *message.Message)

	DoNotBlockPublishUntilSubscriberAck bool
}

func NewTestServices[Response any](t *testing.T, c TestServicesConfig) TestServices[Response] {
	t.Helper()

	logger := watermill.NewStdLogger(true, true)
	marshaler := cqrs.JSONMarshaler{}

	pubSub := gochannel.NewGoChannel(
		gochannel.Config{BlockPublishUntilSubscriberAck: false},
		logger,
	)

	backend, err := requestreply.NewPubSubBackend[Response](
		requestreply.PubSubBackendConfig{
			Publisher: pubSub,
			SubscriberConstructor: func(subscriberContext requestreply.PubSubBackendSubscribeParams) (message.Subscriber, error) {
				assert.NotEmpty(t, subscriberContext.NotificationID)
				assert.NotEmpty(t, subscriberContext.Command)

				return pubSub, nil
			},
			GenerateSubscribeTopic: func(subscriberContext requestreply.PubSubBackendSubscribeParams) (string, error) {
				assert.NotEmpty(t, subscriberContext.NotificationID)
				assert.NotEmpty(t, subscriberContext.Command)

				return "reply", nil
			},
			GeneratePublishTopic: func(subscriberContext requestreply.PubSubBackendPublishParams) (string, error) {
				assert.NotEmpty(t, subscriberContext.NotificationID)
				assert.NotEmpty(t, subscriberContext.Command)
				assert.NotEmpty(t, subscriberContext.CommandMessage)

				return "reply", nil
			},
			Logger: logger,
			ModifyNotificationMessage: func(msg *message.Message, params requestreply.PubSubBackendOnCommandProcessedParams) error {
				// to make it deterministic
				msg.UUID = "1"

				assert.NotEmpty(t, params.NotificationID)
				assert.NotEmpty(t, params.Command)
				assert.NotEmpty(t, params.CommandMessage)

				// to ensure backward compatibility
				if c.AssertNotificationMessage != nil {
					c.AssertNotificationMessage(t, msg)
				}

				return nil
			},
			AckCommandErrors:      !c.DoNotAckOnCommandErrors,
			ListenForReplyTimeout: c.ListenForReplyTimeout,
		},
		requestreply.BackendPubsubJSONMarshaler[Response]{},
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
		Marshaler: marshaler,
		Logger:    logger,
	})
	require.NoError(t, err)

	return TestServices[Response]{
		Logger: logger,
		PubSub: gochannel.NewGoChannel(
			gochannel.Config{BlockPublishUntilSubscriberAck: !c.DoNotBlockPublishUntilSubscriberAck},
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

type TestCommand2 struct {
	ID string `json:"id"`
}

type TestCommandResponse struct {
	ID string `json:"id"`
}

func TestRequestReply_without_response_no_error(t *testing.T) {
	ts := NewTestServices[struct{}](t, TestServicesConfig{
		AssertNotificationMessage: func(t *testing.T, msg *message.Message) {
			assert.NotEmpty(t, msg.Metadata.Get(requestreply.HasErrorMetadataKey))
		},
	})

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
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_without_response_with_error(t *testing.T) {
	ts := NewTestServices[struct{}](t, TestServicesConfig{
		AssertNotificationMessage: func(t *testing.T, msg *message.Message) {
			assert.NotEmpty(t, msg.Metadata.Get(requestreply.HasErrorMetadataKey))
			assert.NotEmpty(t, msg.Metadata.Get(requestreply.ErrorMetadataKey))
		},
	})

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
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_with_response_no_error(t *testing.T) {
	ts := NewTestServices[TestCommandResponse](t, TestServicesConfig{
		AssertNotificationMessage: func(t *testing.T, msg *message.Message) {
			assert.NotEmpty(t, msg.Metadata.Get(requestreply.HasErrorMetadataKey))
			assert.NotEmpty(t, msg.Metadata.Get(requestreply.ResponseMetadataKey))
		},
	})

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
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_with_response_with_error(t *testing.T) {
	ts := NewTestServices[TestCommandResponse](t, TestServicesConfig{})

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
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_without_response_multiple_replies(t *testing.T) {
	ts := NewTestServices[TestCommandResponse](t, TestServicesConfig{
		DoNotAckOnCommandErrors: true,
	})

	i := 0

	err := ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandlerWithResponse[TestCommand, TestCommandResponse](
			"test_handler",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand) (TestCommandResponse, error) {
				i++

				if i == 3 {
					return TestCommandResponse{ID: fmt.Sprintf("%d", i)}, nil
				}

				return TestCommandResponse{ID: fmt.Sprintf("%d", i)}, fmt.Errorf("error %d", i)
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
		assert.EqualValues(t, TestCommandResponse{ID: "1"}, reply.HandlerResponse)

		require.Error(t, reply.HandlerErr)
		assert.Equal(t, "error 1", reply.HandlerErr.Error())

		assert.NotEmpty(t, reply.ReplyMsg.Metadata.Get(requestreply.NotificationIdMetadataKey))
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}

	select {
	case reply := <-replyCh:
		assert.EqualValues(t, TestCommandResponse{ID: "2"}, reply.HandlerResponse)

		require.Error(t, reply.HandlerErr)
		assert.Equal(t, "error 2", reply.HandlerErr.Error())

		assert.NotEmpty(t, reply.ReplyMsg.Metadata.Get(requestreply.NotificationIdMetadataKey))
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}

	select {
	case reply := <-replyCh:
		assert.EqualValues(t, TestCommandResponse{ID: "3"}, reply.HandlerResponse)

		require.NoError(t, reply.HandlerErr)

		assert.NotEmpty(t, reply.ReplyMsg.Metadata.Get(requestreply.NotificationIdMetadataKey))
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_timout(t *testing.T) {
	timeout := time.Millisecond * 10

	ts := NewTestServices[struct{}](t, TestServicesConfig{
		ListenForReplyTimeout: &timeout,
	})

	err := ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandler[TestCommand](
			"test_handler",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand) error {
				time.Sleep(time.Second)
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
		require.Error(t, reply.HandlerErr)
		require.IsType(t, requestreply.ReplyTimeoutError{}, reply.HandlerErr)

		replyTimeoutError := reply.HandlerErr.(requestreply.ReplyTimeoutError)
		assert.Equal(t, context.DeadlineExceeded, replyTimeoutError.Err)
		assert.NotEmpty(t, replyTimeoutError.Duration)
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_context_cancellation(t *testing.T) {
	ts := NewTestServices[struct{}](t, TestServicesConfig{})

	err := ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandler[TestCommand](
			"test_handler",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand) error {
				time.Sleep(time.Second)
				return nil
			},
		),
	)
	require.NoError(t, err)

	ts.RunRouter()

	ctx, cancel := context.WithCancel(context.Background())

	replyCh, err := requestreply.SendAndWait[struct{}](
		ctx,
		ts.CommandBus,
		ts.RequestReplyBackend,
		&TestCommand{ID: "1"},
	)
	require.NoError(t, err)
	require.NotNil(t, replyCh)

	cancel()

	select {
	case reply := <-replyCh:
		assert.Empty(t, reply.HandlerResponse)
		require.Error(t, reply.HandlerErr)
		require.IsType(t, requestreply.ReplyTimeoutError{}, reply.HandlerErr)

		replyTimeoutError := reply.HandlerErr.(requestreply.ReplyTimeoutError)
		assert.Equal(t, context.Canceled, replyTimeoutError.Err)
		assert.NotEmpty(t, replyTimeoutError.Duration)
	case <-time.After(time.Millisecond * 100):
		t.Fatal("timeout")
	}
}

func TestRequestReply_parallel_different_handlers(t *testing.T) {
	ts := NewTestServices[TestCommandResponse](t, TestServicesConfig{
		DoNotAckOnCommandErrors: true,
	})

	err := ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandlerWithResponse[TestCommand, TestCommandResponse](
			"test_handler_1",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand) (TestCommandResponse, error) {

				return TestCommandResponse{ID: cmd.ID}, fmt.Errorf("error 1 %s", cmd.ID)
			},
		),
	)
	require.NoError(t, err)

	err = ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandlerWithResponse[TestCommand2, TestCommandResponse](
			"test_handler_2",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand2) (TestCommandResponse, error) {

				return TestCommandResponse{ID: cmd.ID}, fmt.Errorf("error 2 %s", cmd.ID)
			},
		),
	)
	require.NoError(t, err)

	ts.RunRouter()

	start := make(chan struct{})
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-start

		cmd := TestCommand{ID: watermill.NewUUID()}

		replyCh, err := requestreply.SendAndWait[TestCommandResponse](
			context.Background(),
			ts.CommandBus,
			ts.RequestReplyBackend,
			&cmd,
		)
		require.NoError(t, err)
		require.NotNil(t, replyCh)

		i := 0

		for reply := range replyCh {
			assert.EqualValues(t, TestCommandResponse{ID: cmd.ID}, reply.HandlerResponse)
			require.Error(t, reply.HandlerErr)
			assert.Equal(t, fmt.Sprintf("error 1 %s", cmd.ID), reply.HandlerErr.Error())
			i++

			if i > 100 {
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		<-start

		cmd := TestCommand2{ID: watermill.NewUUID()}

		replyCh, err := requestreply.SendAndWait[TestCommandResponse](
			context.Background(),
			ts.CommandBus,
			ts.RequestReplyBackend,
			&cmd,
		)
		require.NoError(t, err)
		require.NotNil(t, replyCh)

		i := 0

		for reply := range replyCh {
			assert.EqualValues(t, TestCommandResponse{ID: cmd.ID}, reply.HandlerResponse)
			require.Error(t, reply.HandlerErr)
			assert.Equal(t, fmt.Sprintf("error 2 %s", cmd.ID), reply.HandlerErr.Error())
			i++

			if i > 100 {
				return
			}
		}
	}()

	// sync workers
	close(start)
	wg.Wait()
}

func TestRequestReply_parallel_same_handler(t *testing.T) {
	ts := NewTestServices[TestCommandResponse](t, TestServicesConfig{
		DoNotBlockPublishUntilSubscriberAck: true,
	})

	err := ts.CommandProcessor.AddHandlers(
		requestreply.NewCommandHandlerWithResponse[TestCommand, TestCommandResponse](
			"test_handler",
			ts.RequestReplyBackend,
			func(ctx context.Context, cmd *TestCommand) (TestCommandResponse, error) {
				return TestCommandResponse{ID: cmd.ID}, nil
			},
		),
	)
	require.NoError(t, err)

	ts.RunRouter()

	count := 20
	wg := sync.WaitGroup{}
	wg.Add(count)

	start := make(chan struct{})

	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()

			<-start

			cmd := TestCommand{ID: uuid.NewString()}
			replyCh, err := requestreply.SendAndWait[TestCommandResponse](
				context.Background(),
				ts.CommandBus,
				ts.RequestReplyBackend,
				&cmd,
			)
			require.NoError(t, err)
			require.NotNil(t, replyCh)

			select {
			case reply := <-replyCh:
				assert.EqualValues(t, TestCommandResponse{ID: cmd.ID}, reply.HandlerResponse)
				assert.NoError(t, reply.HandlerErr)
			case <-time.After(time.Millisecond * 100):
				t.Fatal("timeout")
			}
		}()
	}

	// sync workers
	close(start)
	wg.Wait()
}
