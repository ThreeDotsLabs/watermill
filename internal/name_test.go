package internal_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/internal"
	"github.com/stretchr/testify/assert"
)

type testStruct struct{}

type stringerStruct struct{}

func (stringerStruct) String() string {
	return "stringer"
}

func TestStructName(t *testing.T) {
	testCases := []struct {
		Name         string
		Struct       interface{}
		ExpectedName string
	}{
		{
			Name:         "simple_struct",
			Struct:       testStruct{},
			ExpectedName: "internal_test.testStruct",
		},
		{
			Name:         "pointer_struct",
			Struct:       &testStruct{},
			ExpectedName: "internal_test.testStruct",
		},
		{
			Name:         "stringer",
			Struct:       stringerStruct{},
			ExpectedName: "stringer",
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			s := internal.StructName(c.Struct)
			assert.Equal(t, c.ExpectedName, s)
		})
	}
}
