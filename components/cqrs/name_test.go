package cqrs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

func TestFullyQualifiedStructName(t *testing.T) {
	type Object struct{}

	assert.Equal(t, "cqrs_test.Object", cqrs.FullyQualifiedStructName(Object{}))
	assert.Equal(t, "cqrs_test.Object", cqrs.FullyQualifiedStructName(&Object{}))
}

func BenchmarkFullyQualifiedStructName(b *testing.B) {
	type Object struct{}
	o := Object{}

	for i := 0; i < b.N; i++ {
		cqrs.FullyQualifiedStructName(o)
	}
}

func TestStructName(t *testing.T) {
	type Object struct{}

	assert.Equal(t, "Object", cqrs.StructName(Object{}))
	assert.Equal(t, "Object", cqrs.StructName(&Object{}))
}

func TestNamedStruct(t *testing.T) {
	assert.Equal(t, "named object", cqrs.NamedStruct(cqrs.StructName)(namedObject{}))
	assert.Equal(t, "named object", cqrs.NamedStruct(cqrs.StructName)(&namedObject{}))

	// Test fallback
	type Object struct{}

	assert.Equal(t, "Object", cqrs.NamedStruct(cqrs.StructName)(Object{}))
	assert.Equal(t, "Object", cqrs.NamedStruct(cqrs.StructName)(&Object{}))
}

type namedObject struct{}

func (namedObject) Name() string {
	return "named object"
}
