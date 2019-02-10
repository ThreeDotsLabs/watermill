package cqrs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

func TestJsonMarshaler(t *testing.T) {
	marshaler := cqrs.JSONMarshaler{}

	eventToMarshal := TestEvent{
		ID:   watermill.NewULID(),
		When: time.Date(2016, time.August, 15, 14, 13, 12, 0, time.UTC),
	}

	msg, err := marshaler.Marshal(eventToMarshal)
	require.NoError(t, err)

	eventToUnmarshal := TestEvent{}
	err = marshaler.Unmarshal(msg, &eventToUnmarshal)
	require.NoError(t, err)

	assert.EqualValues(t, eventToMarshal, eventToUnmarshal)
}
