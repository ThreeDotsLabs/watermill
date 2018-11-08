package middleware

import (
	"math/rand"

	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
)

func shouldFail(errorRatio float32) bool {
	r := rand.Float32()
	return r <= errorRatio
}

func RandomFail(errorRatio float32) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(message *message.Message) ([]*message.Message, error) {
			if shouldFail(errorRatio) {
				return nil, errors.New("random fail occurred")
			}

			return h(message)
		}
	}
}

func RandomPanic(panicRatio float32) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(message *message.Message) ([]*message.Message, error) {
			if shouldFail(panicRatio) {
				panic("random panic occurred")
			}

			return h(message)
		}
	}
}
