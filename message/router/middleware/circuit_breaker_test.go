package middleware_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker(t *testing.T) {
	t.Parallel()

	count := 0
	failing := true

	h := middleware.NewCircuitBreaker(
		gobreaker.Settings{
			Name:    "test",
			Timeout: time.Millisecond * 50,
		},
	).Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		count++

		if failing {
			return nil, errors.New("test error")
		}

		return nil, nil
	})

	msg := message.NewMessage("1", nil)

	// The first 6 calls should fail and increment the count
	for i := 0; i < 6; i++ {
		_, err := h(msg)
		assert.Error(t, err)
	}

	assert.Equal(t, 6, count)

	// The next calls should fail and not increment the count (the circuit breaker is open)
	for i := 0; i < 4; i++ {
		_, err := h(msg)
		assert.Error(t, err)
	}
	assert.Equal(t, 6, count)

	time.Sleep(time.Millisecond * 100)
	failing = false

	// After a timeout, the Circuit Breaker is closed again
	for i := 0; i < 4; i++ {
		_, err := h(msg)
		assert.NoError(t, err)
	}
	assert.Equal(t, 10, count)
}
