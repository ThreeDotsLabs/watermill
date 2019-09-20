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
	ts := NewTestServices()

	captureCommandHandler := &CaptureCommandHandler{}
	captureEventHandler := &CaptureEventHandler{}

	router, cqrsFacade := createRouterAndFacade(ts, t, captureCommandHandler, captureEventHandler)

	pointerCmd := &TestCommand{ID: watermill.NewULID()}
	require.NoError(t, cqrsFacade.CommandBus().Send(context.Background(), pointerCmd))
	assert.EqualValues(t, []interface{}{pointerCmd}, captureCommandHandler.HandledCommands())
	captureCommandHandler.Reset()

	nonPointerCmd := TestCommand{ID: watermill.NewULID()}
	require.NoError(t, cqrsFacade.CommandBus().Send(context.Background(), nonPointerCmd))
	// command is always unmarshaled to pointer value
	assert.EqualValues(t, []interface{}{&nonPointerCmd}, captureCommandHandler.HandledCommands())
	captureCommandHandler.Reset()

	pointerEvent := &TestEvent{ID: watermill.NewULID()}
	require.NoError(t, cqrsFacade.EventBus().Publish(context.Background(), pointerEvent))
	assert.EqualValues(t, []interface{}{pointerEvent}, captureEventHandler.HandledEvents())
	captureEventHandler.Reset()

	nonPointerEvent := TestEvent{ID: watermill.NewULID()}
	require.NoError(t, cqrsFacade.EventBus().Publish(context.Background(), nonPointerEvent))
	// event is always unmarshaled to pointer value
	assert.EqualValues(t, []interface{}{&nonPointerEvent}, captureEventHandler.HandledEvents())
	captureEventHandler.Reset()

	assert.NoError(t, router.Close())

	assert.Equal(t, cqrsFacade.CommandEventMarshaler(), ts.Marshaler)
}

func createRouterAndFacade(ts TestServices, t *testing.T, commandHandler *CaptureCommandHandler, eventHandler *CaptureEventHandler) (*message.Router, *cqrs.Facade) {
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
