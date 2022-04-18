package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/bus"
)

func TestProtobufMarshaler(t *testing.T) {
	marshaler := bus.ProtobufMarshaler{}

	when, err := ptypes.TimestampProto(time.Now())
	require.NoError(t, err)
	eventToMarshal := &TestProtobufEvent{
		Id:   watermill.NewULID(),
		When: when,
	}

	msg, err := marshaler.Marshal(context.Background(), eventToMarshal)
	require.NoError(t, err)

	eventToUnmarshal := &TestProtobufEvent{}
	err = marshaler.Unmarshal(msg, eventToUnmarshal)
	require.NoError(t, err)

	assert.EqualValues(t, eventToMarshal.String(), eventToUnmarshal.String())
	assert.Equal(t, msg.Metadata.Get("name"), "cqrs_test.TestProtobufEvent")
}

func TestProtobufMarshaler_Marshal_generated_name(t *testing.T) {
	marshaler := bus.ProtobufMarshaler{
		NewUUID: func() string {
			return "foo"
		},
	}

	when, err := ptypes.TimestampProto(time.Now())
	require.NoError(t, err)
	eventToMarshal := &TestProtobufEvent{
		Id:   watermill.NewULID(),
		When: when,
	}

	msg, err := marshaler.Marshal(context.Background(), eventToMarshal)
	require.NoError(t, err)

	assert.Equal(t, msg.UUID, "foo")
}
