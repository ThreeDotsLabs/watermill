package cqrs_test

import (
	"testing"

	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"

	"github.com/stretchr/testify/assert"
)

func TestEventBus_Publish_non_pointer(t *testing.T) {
	ts := NewTestServices()
	eventBus := cqrs.NewEventBus(ts.PubSub, "events", ts.Marshaler)

	assert.IsType(t, cqrs.NonPointerError{}, errors.Cause(eventBus.Publish(TestEvent{})))
}
