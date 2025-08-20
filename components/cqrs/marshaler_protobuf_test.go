package cqrs_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

func TestProtoMarshaler(t *testing.T) {
	assertProtoMarshalUnmarshal(
		t,
		cqrs.ProtoMarshaler{},
		cqrs.ProtoMarshaler{},
		newProtoTestComplexEvent(),
		&TestComplexProtobufEvent{},
		"cqrs_test.TestComplexProtobufEvent",
	)
}

func TestProtoMarshaler_Marshal_generated_name(t *testing.T) {
	marshaler := cqrs.ProtoMarshaler{
		NewUUID: func() string {
			return "foo"
		},
	}

	msg, err := marshaler.Marshal(newProtoTestComplexEvent())
	require.NoError(t, err)

	assert.Equal(t, msg.UUID, "foo")
}

// newProtoLegacyTestEvent returns the same event in two different protobuf versions
func newProtoLegacyTestEvent() (*TestProtobufEvent, *TestProtobufLegacyEvent) {
	when := timestamppb.New(time.Now())
	id := watermill.NewULID()

	legacy := &TestProtobufEvent{
		Id:   id,
		When: when,
	}

	regenerated := &TestProtobufLegacyEvent{
		Id:   id,
		When: when,
	}

	return legacy, regenerated
}

func newProtoTestComplexEvent() *TestComplexProtobufEvent {
	when := timestamppb.New(time.Now())

	eventToMarshal := &TestComplexProtobufEvent{
		Id:   watermill.NewULID(),
		Data: []byte("data"),
		When: when,
		NestedMap: map[string]*SubEvent{
			"foo": {
				Tags:  []string{"tag1", "tag2"},
				Flags: map[string]bool{"flag1": true, "flag2": false},
			},
		},
		Events: []*SubEvent{
			{
				Tags: []string{"tag1", "tag2"},
			},
			{
				Tags: []string{"tag3", "tag4"},
			},
		},
		Result: &TestComplexProtobufEvent_Success{
			Success: &SubEvent{
				Tags:  []string{"tag10"},
				Flags: map[string]bool{"flag10": true},
			},
		},
	}
	return eventToMarshal
}

func assertProtoMarshalUnmarshal[T1, T2 fmt.Stringer](
	t *testing.T,
	marshaler cqrs.CommandEventMarshaler,
	unmarshaler cqrs.CommandEventMarshaler,
	eventToMarshal T1,
	eventToUnmarshal T2,
	expectedEventName string,
) {
	t.Helper()

	msg, err := marshaler.Marshal(eventToMarshal)
	require.NoError(t, err)

	err = unmarshaler.Unmarshal(msg, eventToUnmarshal)
	require.NoError(t, err)

	eventToMarshalJson, err := json.Marshal(eventToMarshal)
	require.NoError(t, err)

	eventToUnmarshalJson, err := json.Marshal(eventToUnmarshal)
	require.NoError(t, err)

	assert.JSONEq(t, string(eventToMarshalJson), string(eventToUnmarshalJson))
	assert.Equal(t, expectedEventName, msg.Metadata.Get("name"))
}
