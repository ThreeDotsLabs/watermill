package middleware

import (
	"github.com/roblaszczak/gooddd/message/handler"
	"math/rand"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
)

func shouldFail(errorRatio float32) bool {
	r := rand.Float32()
	return r <= errorRatio
}

func RandomFail(errorRatio float32) handler.Middleware {
	return func(h handler.Func) handler.Func {
		return func(message message.ConsumedMessage) ([]message.ProducedMessage, error) {
			if shouldFail(errorRatio) {
				return nil, errors.New("random fail occurred")
			}

			return h(message)
		}
	}
}

func RandomPanic(panicRatio float32) handler.Middleware {
	return func(h handler.Func) handler.Func {
		return func(message message.ConsumedMessage) ([]message.ProducedMessage, error) {
			if shouldFail(panicRatio) {
				panic("random panic occurred")
			}

			return h(message)
		}
	}
}
