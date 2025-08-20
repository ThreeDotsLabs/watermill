package cqrs_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtobufMarshaler_with_fallback(t *testing.T) {
	marshaler := cqrs.ProtobufMarshaler{}

	assertProtoMarshalUnmarshal(
		t,
		marshaler,
		marshaler,
		newProtoTestComplexEvent(),
		&TestComplexProtobufEvent{},
		"cqrs_test.TestComplexProtobufEvent",
	)

	legacyEvent, _ := newProtoLegacyTestEvent()
	assertProtoMarshalUnmarshal(
		t,
		marshaler,
		marshaler,
		legacyEvent,
		&TestProtobufEvent{},
		"cqrs_test.TestProtobufEvent",
	)
}

func TestProtobufMarshaler_without_fallback_legacy_event(t *testing.T) {
	legacyEvent, _ := newProtoLegacyTestEvent()

	marshaler := cqrs.ProtobufMarshaler{
		DisableStdProtoFallback: true,
	}

	assertProtoMarshalUnmarshal(
		t,
		marshaler,
		marshaler,
		legacyEvent,
		&TestProtobufEvent{},
		"cqrs_test.TestProtobufEvent",
	)
}

func TestProtobufMarshaler_Marshal_generated_name(t *testing.T) {
	marshaler := cqrs.ProtobufMarshaler{
		NewUUID: func() string {
			return "foo"
		},
	}

	msg, err := marshaler.Marshal(newProtoTestComplexEvent())
	require.NoError(t, err)

	assert.Equal(t, msg.UUID, "foo")
}

func TestProtobufMarshaler_catch_panic(t *testing.T) {
	marshalerNoFallback := cqrs.ProtobufMarshaler{
		DisableStdProtoFallback: true,
	}
	marshalerWithFallback := cqrs.ProtobufMarshaler{
		DisableStdProtoFallback: false,
	}

	complexEvent := newProtoTestComplexEvent()

	msg, err := marshalerNoFallback.Marshal(complexEvent)
	assert.Nil(t, msg)
	assert.ErrorContains(t, err, "(we recommend migrating marshaler to cqrs.ProtoMarshaler to avoid that)")
	assert.ErrorContains(t, err, "invalid memory address or nil pointer dereference")
	assert.ErrorContains(t, err, "github.com/gogo/protobuf/proto panic")
	assert.ErrorContains(t, err, "runtime/debug.Stack()", "error should contain stack trace")

	// let's simulate situation when publishing service uses fallback and consuming service does not
	msg, err = marshalerWithFallback.Marshal(complexEvent)
	require.NoError(t, err)

	err = marshalerNoFallback.Unmarshal(msg, &TestComplexProtobufEvent{})
	assert.ErrorContains(t, err, "(we recommend migrating marshaler to cqrs.ProtoMarshaler to avoid that)")
	assert.ErrorContains(t, err, "protobuf tag not enough fields in TestComplexProtobufEvent.state")
	assert.ErrorContains(t, err, "github.com/gogo/protobuf/proto panic")
	assert.ErrorContains(t, err, "runtime/debug.Stack()", "error should contain stack trace")

	// marshaler with fallback should handle this message
	err = marshalerWithFallback.Unmarshal(msg, &TestComplexProtobufEvent{})
	require.NoError(t, err)
}

func TestProtobufMarshaler_compatible_with_ProtoMarshaler(t *testing.T) {
	legacyEvent, legacyEventRegenerated := newProtoLegacyTestEvent()
	complexEvent := newProtoTestComplexEvent()

	deprecatedMarshaler := cqrs.ProtobufMarshaler{}
	newMarshaler := cqrs.ProtoMarshaler{}

	t.Run("from_deprecated_to_new", func(t *testing.T) {
		assertProtoMarshalUnmarshal(
			t,
			deprecatedMarshaler,
			newMarshaler,
			complexEvent,
			&TestComplexProtobufEvent{},
			"cqrs_test.TestComplexProtobufEvent",
		)

		assertProtoMarshalUnmarshal(
			t,
			deprecatedMarshaler,
			newMarshaler,
			legacyEvent,
			&TestProtobufLegacyEvent{},
			"cqrs_test.TestProtobufEvent",
		)
	})

	t.Run("from_new_to_deprecated", func(t *testing.T) {
		assertProtoMarshalUnmarshal(
			t,
			newMarshaler,
			deprecatedMarshaler,
			complexEvent,
			&TestComplexProtobufEvent{},
			"cqrs_test.TestComplexProtobufEvent",
		)

		assertProtoMarshalUnmarshal(
			t,
			newMarshaler,
			deprecatedMarshaler,
			legacyEventRegenerated,
			&TestProtobufEvent{},
			"cqrs_test.TestProtobufLegacyEvent",
		)
	})
}
