package middleware_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	"github.com/ThreeDotsLabs/watermill/message"
)

func TestDuplicator(t *testing.T) {
	messages := []*message.Message{}
	handler := middleware.Duplicator(func(msg *message.Message) ([]*message.Message, error) {
		messages = append(messages, msg)
		return message.Messages{msg}, nil
	})

	msg := message.NewMessage("1", nil)

	_, _ = handler(msg)

	assert.Len(t, messages, 2)
	assert.Equal(t, "1", messages[0].UUID)
	assert.Equal(t, "1", messages[1].UUID)
}
