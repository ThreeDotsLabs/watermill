package middleware_test

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestIgnoreErrors_Middleware(t *testing.T) {
	testCases := []struct {
		Name            string
		IgnoredErrors   []error
		TestError       error
		ShouldBeIgnored bool
	}{
		{
			Name:            "ignored_error",
			IgnoredErrors:   []error{errors.New("test")},
			TestError:       errors.New("test"),
			ShouldBeIgnored: true,
		},
		{
			Name:            "not_ignored_error",
			IgnoredErrors:   []error{errors.New("test")},
			TestError:       errors.New("not_ignored"),
			ShouldBeIgnored: false,
		},
		{
			Name:            "wrapped_error_should_ignore",
			IgnoredErrors:   []error{errors.New("test")},
			TestError:       errors.Wrap(errors.New("test"), "wrapped"),
			ShouldBeIgnored: true,
		},
	}

	for _, c := range testCases {
		t.Run(c.Name, func(t *testing.T) {
			m := middleware.NewIgnoreErrors(c.IgnoredErrors)

			messagesToProduce := []*message.Message{message.NewMessage("1", nil)}

			producedMessages, err := m.Middleware(func(msg *message.Message) ([]*message.Message, error) {
				return messagesToProduce, c.TestError
			})(nil)

			if c.ShouldBeIgnored {
				assert.NoError(t, err)
			} else {
				assert.Equal(t, c.TestError, err)
			}

			assert.Equal(t, messagesToProduce, producedMessages)
		})
	}
}
