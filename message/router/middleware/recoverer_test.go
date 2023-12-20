package middleware_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ascendsoftware/watermill/message"
	"github.com/ascendsoftware/watermill/message/router/middleware"
	"github.com/stretchr/testify/require"
)

func TestRecoverer_Panic(t *testing.T) {
	h := middleware.Recoverer(func(msg *message.Message) (messages []*message.Message, e error) {
		panic("foo")
	})

	_, err := h(message.NewMessage("1", nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "message/router/middleware/recoverer.go") // stacktrace part
}

func TestRecoverer_PanicNil(t *testing.T) {
	h := middleware.Recoverer(func(msg *message.Message) (messages []*message.Message, e error) {
		panic(nil)
	})

	_, err := h(message.NewMessage("1", nil))
	require.Error(t, err)
}

func TestRecoverer_NoPanic(t *testing.T) {
	h := middleware.Recoverer(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, nil
	})

	_, err := h(message.NewMessage("1", nil))
	require.NoError(t, err)
}
