package middleware

import (
	"github.com/roblaszczak/gooddd/handler"
	"github.com/pkg/errors"
)

func Recoverer(h handler.Handler) handler.Handler {
	return func(event handler.Message) (events []handler.MessagePayload, err error) {
		defer func() {
			if r := recover(); r != nil {
				if err == nil {
					err = errors.Errorf("panic occurred: %+v", r)
				} else {
					err = errors.Wrapf(err, "panic occurred: %+v", r)
				}
				return
			}
		}()

		return h(event)
	}
}
