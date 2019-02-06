package middleware_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/stretchr/testify/assert"
)

func TestRandomFail(t *testing.T) {
	h := middleware.RandomFail(1)(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, nil
	})

	_, err := h(message.NewMessage("1", nil))
	assert.Error(t, err)
}

func TestRandomPanic(t *testing.T) {
	h := middleware.RandomPanic(1)(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, nil
	})

	assert.Panics(t, func() {
		_, _ = h(message.NewMessage("1", nil))
	})
}
