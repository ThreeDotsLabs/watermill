package cqrs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type NamedEvent struct {
	TestEvent
}

func (e NamedEvent) Name() string {
	return "testEvent"
}

func TestNameMarshaler(t *testing.T) {
	eventToMarshal := NamedEvent{
		TestEvent: TestEvent{
			ID:   watermill.NewULID(),
			When: time.Date(2016, time.August, 15, 14, 13, 12, 0, time.UTC),
		},
	}

	marshaler := &cqrs.NameMarshaler{}

	msg, err := marshaler.Marshal(eventToMarshal)
	require.NoError(t, err)

	assert.Equal(t, "testEvent", marshaler.NameFromMessage(msg))

	eventToUnmarshal := NamedEvent{}
	err = marshaler.Unmarshal(msg, &eventToUnmarshal)
	require.NoError(t, err)

	assert.EqualValues(t, eventToMarshal, eventToUnmarshal)
	assert.Equal(t, "testEvent", marshaler.Name(eventToUnmarshal))
}

func TestNameMarshaler_NotNamedMessage(t *testing.T) {
	eventToMarshal := TestEvent{
		ID:   watermill.NewULID(),
		When: time.Date(2016, time.August, 15, 14, 13, 12, 0, time.UTC),
	}

	marshaler := &cqrs.NameMarshaler{}

	msg, err := marshaler.Marshal(eventToMarshal)
	require.NoError(t, err)

	eventToUnmarshal := TestEvent{}
	err = marshaler.Unmarshal(msg, &eventToUnmarshal)
	require.NoError(t, err)

	assert.EqualValues(t, eventToMarshal, eventToUnmarshal)
}
