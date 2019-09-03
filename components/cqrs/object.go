package cqrs

import (
	"reflect"
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
