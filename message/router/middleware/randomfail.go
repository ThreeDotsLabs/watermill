package middleware

import (
	"math/rand"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

func shouldFail(probability float32) bool {
	r := rand.Float32()
	return r <= probability
}

// RandomFail makes the handler fail with an error based on random chance. Error probability should be in the range (0,1).
func RandomFail(errorProbability float32) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(message *message.Message) ([]*message.Message, error) {
			if shouldFail(errorProbability) {
				return nil, errors.New("random fail occurred")
			}

			return h(message)
		}
	}
}

// RandomPanic makes the handler panic based on random chance. Panic probability should be in the range (0,1).
func RandomPanic(panicProbability float32) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(message *message.Message) ([]*message.Message, error) {
			if shouldFail(panicProbability) {
				panic("random panic occurred")
			}

			return h(message)
		}
	}
}
