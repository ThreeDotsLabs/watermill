package cqrs_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/stretchr/testify/assert"
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
