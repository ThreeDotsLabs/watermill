package cqrs

import (
	"fmt"
	"reflect"
	"strings"
)

func isPointer(v interface{}) error {
	rv := reflect.ValueOf(v)

	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return NonPointerError{rv.Type()}
	}

	return nil
}

type NonPointerError struct {
	Type reflect.Type
}

func (e NonPointerError) Error() string {
	return "non-pointer command: " + e.Type.String() + ", handler.NewCommand() should return pointer to the command"
}

// ObjectName name returns object name in format [package].[type name].
// It ignores if the value is a pointer or not.
func ObjectName(v interface{}) string {
	s := fmt.Sprintf("%T", v)
	s = strings.TrimLeft(s, "*")

	return s
}
