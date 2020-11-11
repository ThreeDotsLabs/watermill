package middleware_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

var (
	someMsg = message.NewMessage("1", nil)
)

func TestDuplicator(t *testing.T) {
	var executionsCount int
	producedMessages, err := middleware.Duplicator(func(msg *message.Message) ([]*message.Message, error) {
		executionsCount++
		return []*message.Message{msg}, nil
	})(someMsg)

	assert.NoError(t, err)
	assert.Len(t, producedMessages, 2)
	assert.Equal(t, "1", producedMessages[0].UUID)
	assert.Equal(t, "1", producedMessages[1].UUID)
	assert.Equal(t, 2, executionsCount)
}

func TestDuplicator_errors(t *testing.T) {
	_, err := middleware.Duplicator(func(msg *message.Message) ([]*message.Message, error) {
		return nil, errors.New("some error")
	})(someMsg)
	assert.Error(t, err, "some error")

	var wasExecuted bool
	_, err = middleware.Duplicator(func(msg *message.Message) ([]*message.Message, error) {
		if wasExecuted {
			return nil, errors.New("some other error")
		}

		wasExecuted = true
		return []*message.Message{msg}, nil
	})(someMsg)
	assert.Error(t, err, "some other error")
}
