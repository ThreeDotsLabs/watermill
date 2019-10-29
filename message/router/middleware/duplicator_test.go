package middleware_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDuplicator(t *testing.T) {
	someMsg := message.NewMessage("1", nil)

	producedMessages, err := middleware.Duplicator(func(msg *message.Message) ([]*message.Message, error) {
		return []*message.Message{msg}, nil
	})(someMsg)

	assert.NoError(t, err)
	assert.True(t, producedMessages[0].Equals(someMsg))
}

func TestDuplicator_errors(t *testing.T) {
	someMsg := message.NewMessage("1", nil)
	someOtherMsg := message.NewMessage("2", nil)

	testCases := []struct {
		Name                   string
		FirstExecutionError    error
		FirstProducesMessages  []*message.Message
		SecondExecutionError   error
		SecondProducesMessages []*message.Message
		ShouldReturnError      bool
	}{
		{
			Name:                   "equal_errors",
			FirstExecutionError:    errors.New("Some error"),
			FirstProducesMessages:  nil,
			SecondExecutionError:   errors.New("Some error"),
			SecondProducesMessages: nil,
			ShouldReturnError:      false,
		},
		{
			Name:                   "not_equal_errors",
			FirstExecutionError:    errors.New("Some error"),
			FirstProducesMessages:  nil,
			SecondExecutionError:   errors.New("Some other error"),
			SecondProducesMessages: nil,
			ShouldReturnError:      true,
		},
		{
			Name:                   "first_error_nil",
			FirstExecutionError:    nil,
			FirstProducesMessages:  nil,
			SecondExecutionError:   errors.New("Some other error"),
			SecondProducesMessages: nil,
			ShouldReturnError:      true,
		},
		{
			Name:                   "second_error_nil",
			FirstExecutionError:    errors.New("Some error"),
			FirstProducesMessages:  nil,
			SecondExecutionError:   nil,
			SecondProducesMessages: nil,
			ShouldReturnError:      true,
		},
		{
			Name:                   "equal_length_messages_and_equal_messages",
			FirstExecutionError:    nil,
			FirstProducesMessages:  []*message.Message{someMsg},
			SecondExecutionError:   nil,
			SecondProducesMessages: []*message.Message{someMsg},
			ShouldReturnError:      false,
		},
		{
			Name:                   "equal_length_messages_and_not_equal_messages",
			FirstExecutionError:    nil,
			FirstProducesMessages:  []*message.Message{someMsg},
			SecondExecutionError:   nil,
			SecondProducesMessages: []*message.Message{someOtherMsg},
			ShouldReturnError:      true,
		},
		{
			Name:                   "not_equal_length_messages",
			FirstExecutionError:    nil,
			FirstProducesMessages:  []*message.Message{someMsg},
			SecondExecutionError:   nil,
			SecondProducesMessages: []*message.Message{},
			ShouldReturnError:      true,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			wasExecuted := false
			_, err := middleware.Duplicator(func(msg *message.Message) ([]*message.Message, error) {
				if wasExecuted {
					return c.SecondProducesMessages, c.SecondExecutionError
				}

				wasExecuted = true
				return c.FirstProducesMessages, c.FirstExecutionError
			})(message.NewMessage("1", nil))

			if c.ShouldReturnError {
				assert.Error(t, err, "handler is not idempotent")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
