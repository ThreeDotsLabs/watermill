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

	commandHandler := &CaptureCommandHandler{}
	eventHandler := &CaptureEventHandler{}

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	c, err := cqrs.NewCQRS(cqrs.DefaultConfig{
		CommandsTopic: "commands",
		EventsTopic:   "events",
		CommandHandlers: func(_ cqrs.CommandBus, _ cqrs.EventBus) []cqrs.CommandHandler {
			return []cqrs.CommandHandler{commandHandler}
		},
		EventHandlers: func(_ cqrs.CommandBus, _ cqrs.EventBus) []cqrs.EventHandler {
			return []cqrs.EventHandler{eventHandler}
		},
		Router:                router,
		PubSub:                ts.PubSub,
		Logger:                ts.Logger,
		CommandEventMarshaler: ts.Marshaler,
	})

	go func() {
		require.NoError(t, router.Run())
	}()

	<-router.Running()

	cmd := &TestCommand{ID: watermill.NewULID()}
	require.NoError(t, c.CommandBus().Send(cmd))
	assert.EqualValues(t, []*TestCommand{cmd}, commandHandler.HandledCommands())

	event := &TestEvent{ID: watermill.NewULID()}
	require.NoError(t, c.EventBus().Publish(event))
	assert.EqualValues(t, []*TestEvent{event}, eventHandler.HandledEvents())

	assert.NoError(t, router.Close())
}

type TestServices struct {
	Logger    watermill.LoggerAdapter
	PubSub    message.PubSub
	Marshaler cqrs.CommandEventMarshaler
}

func NewTestServices() TestServices {
	logger := watermill.NewStdLogger(true, true)
	pubSub := gochannel.NewGoChannelBlockingUntilAckedByConsumer(0, logger)
	marshaler := cqrs.JsonMarshaler{}

	return TestServices{
		Logger:    logger,
		PubSub:    pubSub,
		Marshaler: marshaler,
	}
}

type TestCommand struct {
	ID string
}

type CaptureCommandHandler struct {
	handledCommands []*TestCommand
}

func (h CaptureCommandHandler) HandledCommands() []*TestCommand {
	return h.handledCommands
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
	handledEvents []*TestEvent
}

func (h CaptureEventHandler) HandledEvents() []*TestEvent {
	return h.handledEvents
}

func (CaptureEventHandler) NewEvent() interface{} {
	return &TestEvent{}
}

func (h *CaptureEventHandler) Handle(cmd interface{}) error {
	h.handledEvents = append(h.handledEvents, cmd.(*TestEvent))
	return nil
}
