package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/bus"
)

var jsonEventToMarshal = TestEvent{
	ID:   watermill.NewULID(),
	When: time.Date(2016, time.August, 15, 14, 13, 12, 0, time.UTC),
}

func TestJsonMarshaler(t *testing.T) {
	marshaler := bus.JSONMarshaler{}

	msg, err := marshaler.Marshal(context.Background(), jsonEventToMarshal)
	require.NoError(t, err)

	eventToUnmarshal := TestEvent{}
	err = marshaler.Unmarshal(msg, &eventToUnmarshal)
	require.NoError(t, err)

	assert.EqualValues(t, jsonEventToMarshal, eventToUnmarshal)
}

func TestJSONMarshaler_Marshal_new_uuid_set(t *testing.T) {
	marshaler := bus.JSONMarshaler{
		NewUUID: func() string {
			return "foo"
		},
	}

	msg, err := marshaler.Marshal(context.Background(), jsonEventToMarshal)
	require.NoError(t, err)

	assert.Equal(t, msg.UUID, "foo")
}

func TestJSONMarshaler_Marshal_generate_name(t *testing.T) {
	marshaler := bus.JSONMarshaler{
		GenerateType: func(v any) string {
			return "foo"
		},
	}

	msg, err := marshaler.Marshal(context.Background(), jsonEventToMarshal)
	require.NoError(t, err)

	assert.Equal(t, msg.Metadata.Get("name"), "foo")
}
