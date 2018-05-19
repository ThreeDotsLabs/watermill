package middleware

import (
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
	"github.com/roblaszczak/gooddd/message/handler"
)

func Recoverer(h handler.HandlerFunc) handler.HandlerFunc {
	return func(event *message.Message) (events []message.Payload, err error) {
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
