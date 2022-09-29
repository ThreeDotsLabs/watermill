package middleware_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/stretchr/testify/require"
)

func TestRecoverer(t *testing.T) {
	h := middleware.Recoverer(func(msg *message.Message) (messages []*message.Message, e error) {
		panic(nil)
	})

	_, err := h(message.NewMessage("1", nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "message/router/middleware/recoverer.go") // stacktrace part
}
