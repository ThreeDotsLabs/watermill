package middleware

import (
	"fmt"
	"runtime/debug"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// RecoveredPanicError holds the recovered panic's error along with the stacktrace.
type RecoveredPanicError struct {
	V          interface{}
	Stacktrace string
}

func (p RecoveredPanicError) Error() string {
	return fmt.Sprintf("panic occurred: %#v, stacktrace: \n%s", p.V, p.Stacktrace)
}

// Recoverer recovers from any panic in the handler and appends RecoveredPanicError with the stacktrace
// to any error returned from the handler.
func Recoverer(h message.HandlerFunc) message.HandlerFunc {
	return func(event *message.Message) (events []*message.Message, err error) {
		panicked := true

		defer func() {
			if r := recover(); r != nil || panicked {
				err = errors.WithStack(RecoveredPanicError{V: r, Stacktrace: string(debug.Stack())})
			}
		}()

		events, err = h(event)
		panicked = false
		return events, err
	}
}
