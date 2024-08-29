package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

// IgnoreErrors provides a middleware that makes the handler ignore some explicitly whitelisted errors.
type IgnoreErrors struct {
	ignoredErrors map[string]struct{}
}

// NewIgnoreErrors creates a new IgnoreErrors middleware.
func NewIgnoreErrors(errs []error) IgnoreErrors {
	errsMap := make(map[string]struct{}, len(errs))

	for _, err := range errs {
		errsMap[err.Error()] = struct{}{}
	}

	return IgnoreErrors{errsMap}
}

// Middleware returns the IgnoreErrors middleware.
func (i IgnoreErrors) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		events, err := h(msg)
		if err != nil {
			if _, ok := i.ignoredErrors[causeError(err)]; ok {
				return events, nil
			}

			return events, err
		}

		return events, nil
	}
}

func causeError(err error) string {
	for {
		switch x := err.(type) {
		case interface{ Unwrap() error }:
			if x.Unwrap() == nil {
				return err.Error()
			}
			err = x.Unwrap()
		default:
			return err.Error()
		}
	}
}
