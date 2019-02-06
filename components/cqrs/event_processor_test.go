package cqrs_test

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"

	"github.com/stretchr/testify/assert"
)

type nonPointerEventProcessor struct {
}

func (nonPointerEventProcessor) NewEvent() interface{} {
	return TestEvent{}
}

func (nonPointerEventProcessor) Handle(cmd interface{}) error {
	panic("not implemented")
}

func TestEventProcessor_non_pointer_event(t *testing.T) {
	ts := NewTestServices()

	eventProcessor := cqrs.NewEventProcessor(
		[]cqrs.EventHandler{nonPointerEventProcessor{}},
		"commands",
		ts.PubSub,
		ts.Marshaler,
		ts.Logger,
	)

	router, err := message.NewRouter(message.RouterConfig{}, ts.Logger)
	require.NoError(t, err)

	err = eventProcessor.AddHandlersToRouter(router)
	assert.IsType(t, cqrs.NonPointerError{}, errors.Cause(err))
}
