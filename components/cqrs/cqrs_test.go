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

	eventConfig := cqrs.EventConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			// todo: assert entire context + assert in different cases
			assert.Equal(t, "cqrs_test.TestEvent", params.EventName)

			return params.EventName, nil
		},
		GenerateHandlerSubscribeTopic: func(params cqrs.GenerateEventHandlerSubscribeTopicParams) (string, error) {
			return params.EventName, nil
		},
		AckOnUnknownEvent: true,
		SubscriberConstructor: func(params cqrs.EventsSubscriberConstructorParams) (message.Subscriber, error) {
			// todo: assert all
			assert.Equal(t, "CaptureEventHandler", params.HandlerName)

			return ts.EventsPubSub, nil
		},
		Marshaler: ts.Marshaler,
		Logger:    ts.Logger,
	}
	eventProcessor, err := cqrs.NewEventProcessorWithConfig(eventConfig)
	require.NoError(t, err)

	eventProcessor.AddHandler(eventHandler)

	err = eventProcessor.AddHandlersToRouter(router)
	require.NoError(t, err)

	eventBus, err := cqrs.NewEventBus(
		ts.EventsPubSub,
		func(eventName string) string {
			assert.Equal(t, "cqrs_test.TestEvent", eventName)

			return eventName
		},
		ts.Marshaler,
	)
	require.NoError(t, err)

	commandConfig := cqrs.CommandConfig{
		GeneratePublishTopic: func(params cqrs.GenerateCommandPublishTopicParams) (string, error) {
			// todo: assert rest of context
			assert.Equal(t, "cqrs_test.TestCommand", params.CommandName)

			return params.CommandName, nil
		},
		GenerateHandlerSubscribeTopic: func(params cqrs.GenerateCommandHandlerSubscribeTopicParams) (string, error) {
			// todo: assert rest of context
			assert.Equal(t, "cqrs_test.TestCommand", params.CommandName)

			return params.CommandName, nil
		},
		SubscriberConstructor: func(params cqrs.CommandsSubscriberConstructorParams) (message.Subscriber, error) {
			assert.Equal(t, "CaptureCommandHandler", params.HandlerName)

			return ts.CommandsPubSub, nil
		},
		Marshaler:                ts.Marshaler,
		Logger:                   ts.Logger,
		AckCommandHandlingErrors: false,
	}

	commandProcessor, err := cqrs.NewCommandProcessorWithConfig(commandConfig)
	require.NoError(t, err)

	commandProcessor.AddHandler(commandHandler)

	err = commandProcessor.AddHandlersToRouter(router)
	require.NoError(t, err)

	commandBus, err := cqrs.NewCommandBusWithConfig(ts.CommandsPubSub, commandConfig)
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
