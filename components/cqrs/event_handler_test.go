package cqrs_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/stretchr/testify/assert"
)

type SomeEvent struct {
	Foo string
}

func TestNewEventHandler(t *testing.T) {
	cmdToSend := &SomeEvent{"bar"}

	ch := cqrs.NewEventHandler(
		"some_handler",
		func(ctx context.Context, cmd *SomeEvent) error {
			assert.Equal(t, cmdToSend, cmd)
			return fmt.Errorf("some error")
		},
	)

	assert.Equal(t, "some_handler", ch.HandlerName())
	assert.Equal(t, &SomeEvent{}, ch.NewEvent())

	err := ch.Handle(context.Background(), cmdToSend)
	assert.EqualError(t, err, "some error")
}

func TestNewGroupEventHandler(t *testing.T) {
	cmdToSend := &SomeEvent{"bar"}

	ch := cqrs.NewGroupEventHandler(
		func(ctx context.Context, cmd *SomeEvent) error {
			assert.Equal(t, cmdToSend, cmd)
			return fmt.Errorf("some error")
		},
	)

	assert.Equal(t, &SomeEvent{}, ch.NewEvent())

	err := ch.Handle(context.Background(), cmdToSend)
	assert.EqualError(t, err, "some error")
}
