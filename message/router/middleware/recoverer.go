package middleware

import (
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

func Recoverer(h message.HandlerFunc) message.HandlerFunc {
	return func(event *message.Message) (events []*message.Message, err error) {
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
