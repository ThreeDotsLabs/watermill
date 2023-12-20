package cqrs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ascendsoftware/watermill"
	"github.com/ascendsoftware/watermill/components/cqrs"
)

func TestProtobufMarshaler(t *testing.T) {
	marshaler := cqrs.ProtobufMarshaler{}

	when := timestamppb.New(time.Now())
	eventToMarshal := &TestProtobufEvent{
		Id:   watermill.NewULID(),
		When: when,
	}

	msg, err := marshaler.Marshal(eventToMarshal)
	require.NoError(t, err)

	eventToUnmarshal := &TestProtobufEvent{}
	err = marshaler.Unmarshal(msg, eventToUnmarshal)
	require.NoError(t, err)

	assert.EqualValues(t, eventToMarshal.String(), eventToUnmarshal.String())
	assert.Equal(t, msg.Metadata.Get("name"), "cqrs_test.TestProtobufEvent")
}

func TestProtobufMarshaler_Marshal_generated_name(t *testing.T) {
	marshaler := cqrs.ProtobufMarshaler{
		NewUUID: func() string {
			return "foo"
		},
	}

	when := timestamppb.New(time.Now())
	eventToMarshal := &TestProtobufEvent{
		Id:   watermill.NewULID(),
		When: when,
	}

	msg, err := marshaler.Marshal(eventToMarshal)
	require.NoError(t, err)

	assert.Equal(t, msg.UUID, "foo")
}
