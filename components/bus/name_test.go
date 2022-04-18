package bus_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/components/bus"
)

func TestFullyQualifiedStructName(t *testing.T) {
	type Object struct{}

	assert.Equal(t, "cqrs_test.Object", bus.FullyQualifiedStructName(Object{}))
	assert.Equal(t, "cqrs_test.Object", bus.FullyQualifiedStructName(&Object{}))
}

func BenchmarkFullyQualifiedStructName(b *testing.B) {
	type Object struct{}
	o := Object{}

	for i := 0; i < b.N; i++ {
		bus.FullyQualifiedStructName(o)
	}
}

func TestStructName(t *testing.T) {
	type Object struct{}

	assert.Equal(t, "Object", bus.StructName(Object{}))
	assert.Equal(t, "Object", bus.StructName(&Object{}))
}

func TestNamedStruct(t *testing.T) {
	assert.Equal(t, "named object", bus.NamedStruct(bus.StructName)(namedObject{}))
	assert.Equal(t, "named object", bus.NamedStruct(bus.StructName)(&namedObject{}))

	// Test fallback
	type Object struct{}

	assert.Equal(t, "Object", bus.NamedStruct(bus.StructName)(Object{}))
	assert.Equal(t, "Object", bus.NamedStruct(bus.StructName)(&Object{}))
}

type namedObject struct{}

func (namedObject) Name() string {
	return "named object"
}
