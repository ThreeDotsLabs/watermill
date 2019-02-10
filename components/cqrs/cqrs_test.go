package cqrs_test

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCQRS is functional test of CQRS command handler and event handler.
func TestCQRS(t *testing.T) {
	ts := NewTestServices()

	captureCommandHandler := &CaptureCommandHandler{}
	captureEventHandler := &CaptureEventHandler{}

	router, cqrsFacade := createRouterAndFacade(ts, t, captureCommandHandler, captureEventHandler)

	pointerCmd := &TestCommand{ID: watermill.NewULID()}
	require.NoError(t, cqrsFacade.CommandBus().Send(pointerCmd))
	assert.EqualValues(t, []interface{}{pointerCmd}, captureCommandHandler.HandledCommands())
	captureCommandHandler.Reset()

	nonPointerCmd := TestCommand{ID: watermill.NewULID()}
	require.NoError(t, cqrsFacade.CommandBus().Send(nonPointerCmd))
	// command is always unmarshaled to pointer value
	assert.EqualValues(t, []interface{}{&nonPointerCmd}, captureCommandHandler.HandledCommands())
	captureCommandHandler.Reset()

	pointerEvent := &TestEvent{ID: watermill.NewULID()}
	require.NoError(t, cqrsFacade.EventBus().Publish(pointerEvent))
	assert.EqualValues(t, []interface{}{pointerEvent}, captureEventHandler.HandledEvents())
	captureEventHandler.Reset()

	nonPointerEvent := TestEvent{ID: watermill.NewULID()}
	require.NoError(t, cqrsFacade.EventBus().Publish(nonPointerEvent))
	// event is always unmarshaled to pointer value
	assert.EqualValues(t, []interface{}{&nonPointerEvent}, captureEventHandler.HandledEvents())
	captureEventHandler.Reset()

	assert.NoError(t, router.Close())
}

func createRouterAndFacade(ts TestServices, t *testing.T, commandHandler *CaptureCommandHandler, eventHandler *CaptureEventHandler) (*message.Router, *cqrs.Facade) {
	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	c, err := cqrs.NewFacade(cqrs.FacadeConfig{
		CommandsTopic: "commands",
		EventsTopic:   "events",
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
		Router:                router,
		CommandsPubSub:        ts.CommandsPubSub,
		EventsPubSub:          ts.EventsPubSub,
		Logger:                ts.Logger,
		CommandEventMarshaler: ts.Marshaler,
	})
	require.NoError(t, err)

	go func() {
		require.NoError(t, router.Run())
	}()

	<-router.Running()

	return router, c
}

type TestServices struct {
	Logger         watermill.LoggerAdapter
	CommandsPubSub message.PubSub
	EventsPubSub   message.PubSub
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

func (h CaptureCommandHandler) HandledCommands() []interface{} {
	return h.handledCommands
}

func (h *CaptureCommandHandler) Reset() {
	h.handledCommands = nil
}

func (CaptureCommandHandler) NewCommand() interface{} {
	return &TestCommand{}
}

func (h *CaptureCommandHandler) Handle(cmd interface{}) error {
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

func (h CaptureEventHandler) HandledEvents() []interface{} {
	return h.handledEvents
}

func (h *CaptureEventHandler) Reset() {
	h.handledEvents = nil
}

func (CaptureEventHandler) NewEvent() interface{} {
	return &TestEvent{}
}

func (h *CaptureEventHandler) Handle(cmd interface{}) error {
	h.handledEvents = append(h.handledEvents, cmd.(*TestEvent))
	return nil
}
