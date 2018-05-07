package middleware

import (
	"github.com/roblaszczak/gooddd/handler"
	"fmt"
	"github.com/roblaszczak/gooddd/domain"
	"runtime/debug"
)

func Recoverer(h handler.Handler) handler.Handler {
	return func(event domain.Event) ([]domain.EventPayload, error) {
		defer func() {
			if r := recover(); r != nil {
				// todo - better log
				fmt.Println("panic occurred: ", r, debug.Stack())
				debug.PrintStack()
			}
		}()

		return h(event)
	}
}
