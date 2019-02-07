package middleware

import (
	"fmt"
	"runtime/debug"

	"github.com/hashicorp/go-multierror"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type RecoveredPanicError struct {
	V          interface{}
	Stacktrace string
}

func (p RecoveredPanicError) Error() string {
	return fmt.Sprintf("panic occurred: %#v, stacktrace: \n%s", p.V, p.Stacktrace)
}

func Recoverer(h message.HandlerFunc) message.HandlerFunc {
	return func(event *message.Message) (events []*message.Message, err error) {
		defer func() {
			if r := recover(); r != nil {
				panicErr := errors.WithStack(RecoveredPanicError{V: r, Stacktrace: string(debug.Stack())})
				err = multierror.Append(err, panicErr)
			}
		}()

		return h(event)
	}
}
