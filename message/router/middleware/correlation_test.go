package middleware_test

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	"github.com/ThreeDotsLabs/watermill/message"
)

func TestCorrelationID(t *testing.T) {
	handlerErr := errors.New("foo")

	handler := middleware.CorrelationID(func(msg *message.Message) ([]*message.Message, error) {
		return message.Messages{message.NewMessage("2", nil)}, handlerErr
	})

	msg := message.NewMessage("1", nil)
	middleware.SetCorrelationID("correlation_id", msg)

	producedMsgs, err := handler(msg)

	assert.Equal(t, middleware.MessageCorrelationID(producedMsgs[0]), "correlation_id")
	assert.Equal(t, handlerErr, err)
}
