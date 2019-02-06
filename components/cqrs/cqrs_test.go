package cqrs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel"
)

type TestServices struct {
	CommandsTopic string
	EventsTopic   string

	Logger    watermill.LoggerAdapter
	PubSub    message.PubSub
	Marshaler cqrs.Marshaler

	CommandBus cqrs.CommandBus
	EventBus   cqrs.EventBus
}

func NewTestServices() TestServices {
	commandsTopic := "commands"
	eventsTopic := "events"

	logger := watermill.NewStdLogger(true, true)
	pubSub := gochannel.NewGoChannelBlockingUntilAckedByConsumer(0, logger)
	marshaler := cqrs.JsonMarshaler{}

	commandBus := cqrs.NewCommandBus(pubSub, commandsTopic, marshaler)
	eventBus := cqrs.NewEventBus(pubSub, eventsTopic, marshaler)

	return TestServices{
		CommandsTopic: commandsTopic,
		EventsTopic:   eventsTopic,

		Logger:     logger,
		PubSub:     pubSub,
		Marshaler:  marshaler,
		CommandBus: commandBus,
		EventBus:   eventBus,
	}
}

type TestCommand struct {
	ID string
}

type TestCommandHandler struct {
	handledCommands []*TestCommand
}

func (h TestCommandHandler) HandledCommands() []*TestCommand {
	return h.handledCommands
}

func (TestCommandHandler) NewCommand() interface{} {
	return &TestCommand{}
}

func (h *TestCommandHandler) Handle(cmd interface{}) error {
	h.handledCommands = append(h.handledCommands, cmd.(*TestCommand))
	return nil
}

// TestCQRS_command_handler is functional test of CQRS command handler.
func TestCQRS_command_handler(t *testing.T) {
	ts := NewTestServices()

	commandHandler := &TestCommandHandler{}
	commandProcessor := cqrs.NewCommandProcessor(
		[]cqrs.CommandHandler{commandHandler},
		ts.CommandsTopic,
		ts.PubSub,
		ts.Marshaler,
		ts.Logger,
	)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	require.NoError(t, commandProcessor.AddHandlersToRouter(router))

	// process is complicated a bit? create command bus, router, add command bus to router, run router in exact order aaand <-router.Running()
	// todo - simplify?
	go func() {
		require.NoError(t, router.Run())
	}()

	<-router.Running()

	cmd := &TestCommand{ID: watermill.NewULID()}
	require.NoError(t, ts.CommandBus.Send(cmd))

	assert.EqualValues(t, []*TestCommand{cmd}, commandHandler.HandledCommands())
	assert.NoError(t, router.Close())
}

type TestEvent struct {
	ID   string
	When time.Time
}

type TestEventHandler struct {
	handledEvents []*TestEvent
}

func (h TestEventHandler) HandledEvents() []*TestEvent {
	return h.handledEvents
}

func (TestEventHandler) NewEvent() interface{} {
	return &TestEvent{}
}

func (h *TestEventHandler) Handle(cmd interface{}) error {
	h.handledEvents = append(h.handledEvents, cmd.(*TestEvent))
	return nil
}

// TestCQRS_event_handler is functional test of CQRS event handler.
func TestCQRS_event_handler(t *testing.T) {
	ts := NewTestServices()

	eventHandler := &TestEventHandler{}
	eventProcessor := cqrs.NewEventProcessor(
		[]cqrs.EventHandler{eventHandler},
		ts.EventsTopic,
		ts.PubSub,
		ts.Marshaler,
		ts.Logger,
	)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	require.NoError(t, eventProcessor.AddHandlersToRouter(router))

	// process is complicated a bit? create command bus, router, add command bus to router, run router in exact order aaand <-router.Running()
	// todo - simplify?
	go func() {
		require.NoError(t, router.Run())
	}()

	<-router.Running()

	cmd := &TestEvent{ID: watermill.NewULID()}
	require.NoError(t, ts.EventBus.Publish(cmd))

	assert.EqualValues(t, []*TestEvent{cmd}, eventHandler.HandledEvents())
	assert.NoError(t, router.Close())
}
