package cqrs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

func TestObjectName(t *testing.T) {
	type Object struct{}

	assert.Equal(t, "cqrs_test.Object", cqrs.ObjectName(Object{}))
	assert.Equal(t, "cqrs_test.Object", cqrs.ObjectName(&Object{}))
}

func BenchmarkObjectName(b *testing.B) {
	type Object struct{}
	o := Object{}

	for i := 0; i < b.N; i++ {
		cqrs.ObjectName(o)
	}
}

func TestStructName(t *testing.T) {
	type Object struct{}

	assert.Equal(t, "Object", cqrs.StructName(Object{}))
	assert.Equal(t, "Object", cqrs.StructName(&Object{}))
}

func TestMessageName(t *testing.T) {
	assert.Equal(t, "named object", cqrs.MessageName(namedObject{}))
	assert.Equal(t, "named object", cqrs.MessageName(&namedObject{}))
}

type namedObject struct{}

func (namedObject) Name() string {
	return "named object"
}
