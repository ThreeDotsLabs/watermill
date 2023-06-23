package cqrs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

// TestCQRS is functional test of CQRS command handler and event handler.
func TestCQRS(t *testing.T) {
	testCases := []struct {
		Name       string
		CreateCqrs func(t *testing.T, cc *CaptureCommandHandler, ce *CaptureEventHandler) (*message.Router, *cqrs.CommandBus, *cqrs.EventBus)
	}{
		{
			// facade is deprecated, testing backwards compatibility
			Name: "facade",
			CreateCqrs: func(t *testing.T, cc *CaptureCommandHandler, ce *CaptureEventHandler) (*message.Router, *cqrs.CommandBus, *cqrs.EventBus) {
				router, cqrsFacade := createRouterAndFacade(t, cc, ce)
				return router, cqrsFacade.CommandBus(), cqrsFacade.EventBus()
			},
		},
		{
			Name:       "constructors",
			CreateCqrs: createCqrsComponents,
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			captureCommandHandler := &CaptureCommandHandler{}
			captureEventHandler := &CaptureEventHandler{}

			router, commandBus, eventBus := tc.CreateCqrs(t, captureCommandHandler, captureEventHandler)

			pointerCmd := &TestCommand{ID: watermill.NewULID()}
			require.NoError(t, commandBus.Send(context.Background(), pointerCmd))
			assert.EqualValues(t, []interface{}{pointerCmd}, captureCommandHandler.HandledCommands())
			captureCommandHandler.Reset()

			nonPointerCmd := TestCommand{ID: watermill.NewULID()}
			require.NoError(t, commandBus.Send(context.Background(), nonPointerCmd))
			// command is always unmarshaled to pointer value
			assert.EqualValues(t, []interface{}{&nonPointerCmd}, captureCommandHandler.HandledCommands())
			captureCommandHandler.Reset()

			pointerEvent := &TestEvent{ID: watermill.NewULID()}
			require.NoError(t, eventBus.Publish(context.Background(), pointerEvent))
			assert.EqualValues(t, []interface{}{pointerEvent}, captureEventHandler.HandledEvents())
			captureEventHandler.Reset()

			nonPointerEvent := TestEvent{ID: watermill.NewULID()}
			require.NoError(t, eventBus.Publish(context.Background(), nonPointerEvent))
			// event is always unmarshaled to pointer value
			assert.EqualValues(t, []interface{}{&nonPointerEvent}, captureEventHandler.HandledEvents())
			captureEventHandler.Reset()

			assert.NoError(t, router.Close())
		})
	}
}

func createCqrsComponents(t *testing.T, commandHandler *CaptureCommandHandler, eventHandler *CaptureEventHandler) (*message.Router, *cqrs.CommandBus, *cqrs.EventBus) {
	ts := NewTestServices()

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	eventProcessor, err := cqrs.NewEventProcessorWithConfig(
		router,
		cqrs.EventProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventProcessorGenerateSubscribeTopicParams) (string, error) {
				return params.EventName, nil
			},
			AckOnUnknownEvent: true,
			SubscriberConstructor: func(params cqrs.EventProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				assert.Equal(t, "CaptureEventHandler", params.HandlerName)

				assert.Implements(t, new(cqrs.EventHandler), params.EventHandler)
				assert.NotNil(t, params.EventHandler)

				return ts.EventsPubSub, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	err = eventProcessor.AddHandlers(eventHandler)
	require.NoError(t, err)

	eventBus, err := cqrs.NewEventBusWithConfig(
		ts.EventsPubSub,
		cqrs.EventBusConfig{
			GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
				assert.Equal(t, "cqrs_test.TestEvent", params.EventName)

				switch cmd := params.Event.(type) {
				case *TestEvent:
					assert.NotEmpty(t, cmd.ID)
				case TestEvent:
					assert.NotEmpty(t, cmd.ID)
				default:
					assert.Fail(t, "unexpected command type: %T", cmd)
				}

				assert.NotEmpty(t, params.Event)

				return params.EventName, nil
			},
			Marshaler: ts.Marshaler,
			Logger:    ts.Logger,
		},
	)
	require.NoError(t, err)

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(
		router,
		cqrs.CommandProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.CommandProcessorGenerateSubscribeTopicParams) (string, error) {
				assert.Equal(t, "cqrs_test.TestCommand", params.CommandName)

				assert.Implements(t, new(cqrs.CommandHandler), params.CommandHandler)
				assert.NotNil(t, params.CommandHandler)

				return params.CommandName, nil
			},
			SubscriberConstructor: func(params cqrs.CommandProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				assert.Equal(t, "CaptureCommandHandler", params.HandlerName)

				return ts.CommandsPubSub, nil
			},
			Marshaler:                ts.Marshaler,
			Logger:                   ts.Logger,
			AckCommandHandlingErrors: false,
		},
	)
	require.NoError(t, err)

	err = commandProcessor.AddHandlers(commandHandler)
	require.NoError(t, err)

	commandBus, err := cqrs.NewCommandBusWithConfig(ts.CommandsPubSub, cqrs.CommandBusConfig{
		GeneratePublishTopic: func(params cqrs.CommandBusGeneratePublishTopicParams) (string, error) {
			assert.Equal(t, "cqrs_test.TestCommand", params.CommandName)

			switch cmd := params.Command.(type) {
			case *TestCommand:
				assert.NotEmpty(t, cmd.ID)
			case TestCommand:
				assert.NotEmpty(t, cmd.ID)
			default:
				assert.Fail(t, "unexpected command type: %T", cmd)
			}

			assert.NotNil(t, params.Command)

			return params.CommandName, nil
		},
		Marshaler: ts.Marshaler,
		Logger:    ts.Logger,
	})
	require.NoError(t, err)

	go func() {
		require.NoError(t, router.Run(context.Background()))
	}()

	<-router.Running()

	return router, commandBus, eventBus
}

func createRouterAndFacade(t *testing.T, commandHandler *CaptureCommandHandler, eventHandler *CaptureEventHandler) (*message.Router, *cqrs.Facade) {
	ts := NewTestServices()

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	c, err := cqrs.NewFacade(cqrs.FacadeConfig{
		GenerateCommandsTopic: func(commandName string) string {
			assert.Equal(t, "cqrs_test.TestCommand", commandName)

			return commandName
		},
		GenerateEventsTopic: func(eventName string) string {
			assert.Equal(t, "cqrs_test.TestEvent", eventName)

			return eventName
		},
		CommandHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.CommandHandler {
			require.NotNil(t, cb)
			require.NotNil(t, eb)

			return []cqrs.CommandHandler{commandHandler}
		},
		EventHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.EventHandler {
			require.NotNil(t, cb)
			require.NotNil(t, eb)

			return []cqrs.EventHandler{eventHandler}
		},
		Router:            router,
		CommandsPublisher: ts.CommandsPubSub,
		CommandsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			assert.Equal(t, "CaptureCommandHandler", handlerName)

			return ts.CommandsPubSub, nil
		},
		EventsPublisher: ts.EventsPubSub,
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			assert.Equal(t, "CaptureEventHandler", handlerName)

			return ts.EventsPubSub, nil
		},
		Logger:                ts.Logger,
		CommandEventMarshaler: ts.Marshaler,
	})
	require.NoError(t, err)

	go func() {
		require.NoError(t, router.Run(context.Background()))
	}()

	<-router.Running()

	assert.Equal(t, c.CommandEventMarshaler(), ts.Marshaler)

	return router, c
}

type TestServices struct {
	Logger         watermill.LoggerAdapter
	CommandsPubSub *gochannel.GoChannel
	EventsPubSub   *gochannel.GoChannel
	Marshaler      cqrs.CommandEventMarshaler
}

func NewTestServices() TestServices {
	logger := watermill.NewStdLogger(true, true)

	return TestServices{
		Logger: logger,
		CommandsPubSub: gochannel.NewGoChannel(
			gochannel.Config{BlockPublishUntilSubscriberAck: true},
			logger,
		),
		EventsPubSub: gochannel.NewGoChannel(
			gochannel.Config{BlockPublishUntilSubscriberAck: true},
			logger,
		),
		Marshaler: cqrs.JSONMarshaler{},
	}
}

type TestCommand struct {
	ID string
}

type CaptureCommandHandler struct {
	handledCommands []interface{}
}

func (h CaptureCommandHandler) HandlerName() string {
	return "CaptureCommandHandler"
}

func (h CaptureCommandHandler) HandledCommands() []interface{} {
	return h.handledCommands
}

func (h *CaptureCommandHandler) Reset() {
	h.handledCommands = nil
}

func (CaptureCommandHandler) NewCommand() interface{} {
	return &TestCommand{}
}

func (h *CaptureCommandHandler) Handle(ctx context.Context, cmd interface{}) error {
	h.handledCommands = append(h.handledCommands, cmd.(*TestCommand))
	return nil
}

type TestEvent struct {
	ID   string
	When time.Time
}

type AnotherTestEvent struct {
	ID string
}

type CaptureEventHandler struct {
	handledEvents []interface{}
}

func (h CaptureEventHandler) HandlerName() string {
	return "CaptureEventHandler"
}

func (h CaptureEventHandler) HandledEvents() []interface{} {
	return h.handledEvents
}

func (h *CaptureEventHandler) Reset() {
	h.handledEvents = nil
}

func (CaptureEventHandler) NewEvent() interface{} {
	return &TestEvent{}
}

func (h *CaptureEventHandler) Handle(ctx context.Context, event interface{}) error {
	h.handledEvents = append(h.handledEvents, event.(*TestEvent))
	return nil
}

type assertPublishTopicPublisher struct {
	ExpectedTopic string
	T             *testing.T
}

func (a assertPublishTopicPublisher) Publish(topic string, messages ...*message.Message) error {
	assert.Equal(a.T, a.ExpectedTopic, topic)
	return nil
}

func (assertPublishTopicPublisher) Close() error {
	return nil
}

type publisherStub struct {
	messages map[string]message.Messages

	mu sync.Mutex
}

func newPublisherStub() *publisherStub {
	return &publisherStub{
		messages: make(map[string]message.Messages),
	}
}

func (*publisherStub) Close() error {
	return nil
}

func (p *publisherStub) Publish(topic string, messages ...*message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.messages[topic] = append(p.messages[topic], messages...)

	return nil
}

func TestFacadeConfig_Validate(t *testing.T) {
	ts := NewTestServices()

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	validConfig := cqrs.FacadeConfig{
		GenerateCommandsTopic: func(commandName string) string {
			return commandName
		},
		GenerateEventsTopic: func(eventName string) string {
			return eventName
		},
		CommandHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.CommandHandler {
			return []cqrs.CommandHandler{}
		},
		EventHandlers: func(cb *cqrs.CommandBus, eb *cqrs.EventBus) []cqrs.EventHandler {
			return []cqrs.EventHandler{}
		},
		Router:            router,
		CommandsPublisher: ts.CommandsPubSub,
		CommandsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			return ts.CommandsPubSub, nil
		},
		EventsPublisher: ts.EventsPubSub,
		EventsSubscriberConstructor: func(handlerName string) (message.Subscriber, error) {
			return ts.EventsPubSub, nil
		},
		Logger:                ts.Logger,
		CommandEventMarshaler: ts.Marshaler,
	}

	testCases := []struct {
		Name   string
		Config cqrs.FacadeConfig
		Valid  bool
	}{
		{
			Name:   "valid",
			Config: validConfig,
			Valid:  true,
		},
		{
			Name: "missing_GenerateCommandsTopic",
			Config: transformConfig(validConfig, func(config *cqrs.FacadeConfig) {
				config.GenerateCommandsTopic = nil
			}),
			Valid: false,
		},
		{
			Name: "missing_CommandsSubscriberConstructor",
			Config: transformConfig(validConfig, func(config *cqrs.FacadeConfig) {
				config.CommandsSubscriberConstructor = nil
			}),
			Valid: false,
		},
		{
			Name: "missing_CommandsPublisher",
			Config: transformConfig(validConfig, func(config *cqrs.FacadeConfig) {
				config.CommandsPublisher = nil
			}),
			Valid: false,
		},
		{
			Name: "missing_GenerateEventsTopic",
			Config: transformConfig(validConfig, func(config *cqrs.FacadeConfig) {
				config.GenerateEventsTopic = nil
			}),
			Valid: false,
		},
		{
			Name: "missing_GenerateEventsTopic",
			Config: transformConfig(validConfig, func(config *cqrs.FacadeConfig) {
				config.EventsSubscriberConstructor = nil
			}),
			Valid: false,
		},
		{
			Name: "missing_EventsPublisher",
			Config: transformConfig(validConfig, func(config *cqrs.FacadeConfig) {
				config.EventsPublisher = nil
			}),
			Valid: false,
		},
		{
			Name: "missing_Router",
			Config: transformConfig(validConfig, func(config *cqrs.FacadeConfig) {
				config.Router = nil
			}),
			Valid: false,
		},
		{
			Name: "missing_Logger",
			Config: transformConfig(validConfig, func(config *cqrs.FacadeConfig) {
				config.Logger = nil
			}),
			Valid: false,
		},
		{
			Name: "missing_CommandEventMarshaler",
			Config: transformConfig(validConfig, func(config *cqrs.FacadeConfig) {
				config.CommandEventMarshaler = nil
			}),
			Valid: false,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			if c.Valid {
				assert.NoError(t, c.Config.Validate())
			} else {
				assert.Error(t, c.Config.Validate())
			}
		})
	}
}

func transformConfig(config cqrs.FacadeConfig, transformFn func(config *cqrs.FacadeConfig)) cqrs.FacadeConfig {
	transformFn(&config)
	return config
}
