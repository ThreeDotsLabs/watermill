package cqrs_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/stretchr/testify/assert"
)

type SomeCommand struct {
	Foo string
}

func TestNewCommandHandler(t *testing.T) {
	cmdToSend := &SomeCommand{"bar"}

	ch := cqrs.NewCommandHandler(
		"some_handler",
		func(ctx context.Context, cmd *SomeCommand) error {
			assert.Equal(t, cmdToSend, cmd)
			return fmt.Errorf("some error")
		},
	)

	assert.Equal(t, "some_handler", ch.HandlerName())
	assert.Equal(t, &SomeCommand{}, ch.NewCommand())

	err := ch.Handle(context.Background(), cmdToSend)
	assert.EqualError(t, err, "some error")
}
