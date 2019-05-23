package cqrs

import (
	"fmt"
	"strings"
)

// ObjectName name returns object name in format [package].[type name].
// It ignores if the value is a pointer or not.
func ObjectName(v interface{}) string {
	s := fmt.Sprintf("%T", v)
	s = strings.TrimLeft(s, "*")

	return s
}

// StructName name returns struct name in format [type name].
// It ignores if the value is a pointer or not.
func StructName(v interface{}) string {
	segments := strings.Split(fmt.Sprintf("%T", v), ".")

	return segments[len(segments)-1]
}

type namedMessage interface {
	Name() string
}

// MessageName returns the name from a message implementing the following interface:
//		type namedMessage interface {
//			Name() string
//		}
// It ignores if the value is a pointer or not.
func MessageName(v interface{}) string {
	if v, ok := v.(namedMessage); ok {
		return v.Name()
	}

	return ""
}
