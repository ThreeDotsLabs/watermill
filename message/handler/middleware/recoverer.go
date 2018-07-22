package middleware

import (
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
	"github.com/roblaszczak/gooddd/message/handler"
)

func Recoverer(h handler.Func) handler.Func {
	return func(event message.ConsumedMessage) (events []message.ProducedMessage, err error) {
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
