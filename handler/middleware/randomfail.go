package middleware

import (
	"github.com/roblaszczak/gooddd/handler"
	"math/rand"
	"github.com/pkg/errors"
)

func shouldFail(errorRatio float32) bool {
	r := rand.Float32()
	return r <= errorRatio
}

func RandomFail(errorRatio float32) handler.Middleware {
	return func(h handler.Handler) handler.Handler {
		return func(message handler.Message) ([]handler.MessagePayload, error) {
			if shouldFail(errorRatio) {
				return nil, errors.New("random fail occurred")
			}

			return h(message)
		}
	}
}

func RandomPanic(panicRatio float32) handler.Middleware {
	return func(h handler.Handler) handler.Handler {
		return func(message handler.Message) ([]handler.MessagePayload, error) {
			if shouldFail(panicRatio) {
				panic("random panic occurred")
			}

			return h(message)
		}
	}
}
