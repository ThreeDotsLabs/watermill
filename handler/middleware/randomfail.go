package middleware

import (
	"github.com/roblaszczak/gooddd/handler"
	"github.com/roblaszczak/gooddd/domain"
	"math/rand"
	"github.com/pkg/errors"
)

func shouldFail(errorRatio float32) bool {
	r := rand.Float32()
	return r <= errorRatio
}

func RandomFail(errorRatio float32) handler.Middleware {
	return func(h handler.Handler) handler.Handler {
		return func(event domain.Event) ([]domain.EventPayload, error) {
			if shouldFail(errorRatio) {
				return nil, errors.New("random fail occurred")
			}

			return h(event)
		}
	}
}

func RandomPanic(panicRatio float32) handler.Middleware {
	return func(h handler.Handler) handler.Handler {
		return func(event domain.Event) ([]domain.EventPayload, error) {
			if shouldFail(panicRatio) {
				panic("random panic occurred")
			}

			return h(event)
		}
	}
}
