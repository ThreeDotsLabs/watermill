package bus

import (
	"fmt"
	"strings"
)

// FullyQualifiedStructName name returns object name in format [package].[type name].
// It ignores if the value is a pointer or not.
func FullyQualifiedStructName(v any) string {
	s := fmt.Sprintf("%T", v)
	s = strings.TrimLeft(s, "*")

	return s
}

// StructName name returns struct name in format [type name].
// It ignores if the value is a pointer or not.
func StructName(v any) string {
	segments := strings.Split(fmt.Sprintf("%T", v), ".")

	return segments[len(segments)-1]
}

type namedStruct interface {
	Name() string
}

// NamedStruct returns the name from a message implementing the following interface:
// 		type namedStruct interface {
// 			Name() string
// 		}
// It ignores if the value is a pointer or not.
func NamedStruct(fallback func(v any) string) func(v any) string {
	return func(v any) string {
		if v, ok := v.(namedStruct); ok {
			return v.Name()
		}

		return fallback(v)
	}
}
