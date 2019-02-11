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

func ObjectName(v interface{}) string {
	s := fmt.Sprintf("%T", v)
	s = strings.TrimLeft(s, "*")

	return s
}
