package middleware_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

func TestTimeout(t *testing.T) {
	timeout := middleware.Timeout(time.Millisecond * 10)

	h := timeout(func(msg *message.Message) ([]*message.Message, error) {
		delay := time.After(time.Millisecond * 100)

		select {
		case <-msg.Context().Done():
			return nil, nil
		case <-delay:
			return nil, errors.New("timeout did not occur")
		}
	})

	msg := message.NewMessage("any-uuid", nil)

	_, err := h(msg)
	require.NoError(t, err)
	assert.Nil(t, msg.Context().Err())
}
