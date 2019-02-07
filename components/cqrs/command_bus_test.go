package cqrs_test

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"

	"github.com/stretchr/testify/assert"
)

func TestCommandBus_Send_non_pointer(t *testing.T) {
	ts := NewTestServices()
	commandBus := cqrs.NewCommandBus(ts.PubSub, "commands", ts.Marshaler)

	assert.IsType(t, cqrs.NonPointerError{}, errors.Cause(commandBus.Send(TestCommand{})))
}
