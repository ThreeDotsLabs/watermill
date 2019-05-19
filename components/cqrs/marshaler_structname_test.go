package cqrs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

func TestStructNameMarshaler(t *testing.T) {
	eventToMarshal := TestEvent{
		ID:   watermill.NewULID(),
		When: time.Date(2016, time.August, 15, 14, 13, 12, 0, time.UTC),
	}

	marshaler := &cqrs.StructNameMarshaler{}

	msg, err := marshaler.Marshal(eventToMarshal)
	require.NoError(t, err)

	assert.Equal(t, "TestEvent", marshaler.NameFromMessage(msg))

	eventToUnmarshal := TestEvent{}
	err = marshaler.Unmarshal(msg, &eventToUnmarshal)
	require.NoError(t, err)

	assert.EqualValues(t, eventToMarshal, eventToUnmarshal)
	assert.Equal(t, "TestEvent", marshaler.Name(eventToUnmarshal))
}
