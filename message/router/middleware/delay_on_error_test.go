package middleware_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

func TestDelayOnError(t *testing.T) {
	m := middleware.DelayOnError{
		InitialInterval: time.Second,
		MaxInterval:     time.Second * 10,
		Multiplier:      2,
	}

	msg := message.NewMessage("1", []byte("test"))

	getDelayFor := func(msg *message.Message) string {
		return msg.Metadata.Get(delay.DelayedForKey)
	}

	okHandler := func(msg *message.Message) ([]*message.Message, error) {
		return nil, nil
	}

	errHandler := func(msg *message.Message) ([]*message.Message, error) {
		return nil, errors.New("error")
	}

	assert.Equal(t, "", getDelayFor(msg))

	_, _ = m.Middleware(okHandler)(msg)
	assert.Equal(t, "", getDelayFor(msg))

	_, _ = m.Middleware(errHandler)(msg)
	assert.Equal(t, "1s", getDelayFor(msg))

	_, _ = m.Middleware(errHandler)(msg)
	assert.Equal(t, "2s", getDelayFor(msg))

	_, _ = m.Middleware(errHandler)(msg)
	assert.Equal(t, "4s", getDelayFor(msg))

	_, _ = m.Middleware(errHandler)(msg)
	assert.Equal(t, "8s", getDelayFor(msg))

	_, _ = m.Middleware(errHandler)(msg)
	assert.Equal(t, "10s", getDelayFor(msg))

	_, _ = m.Middleware(errHandler)(msg)
	assert.Equal(t, "10s", getDelayFor(msg))
}
