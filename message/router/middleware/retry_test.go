package middleware_test

import (
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

func TestRetry_retry(t *testing.T) {
	retry := middleware.Retry{
		MaxRetries: 1,
	}

	runCount := 0
	producedMessages := message.Messages{message.NewMessage("2", nil)}

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		runCount++
		if runCount == 0 {
			return nil, errors.New("foo")
		}

		return producedMessages, nil
	})

	handlerMessages, handlerErr := h(message.NewMessage("1", nil))

	assert.Equal(t, 1, runCount)
	assert.EqualValues(t, producedMessages, handlerMessages)
	assert.NoError(t, handlerErr)
}

func TestRetry_max_retries(t *testing.T) {
	retry := middleware.Retry{
		MaxRetries: 1,
	}

	runCount := 0

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		runCount++
		return nil, errors.New("foo")
	})

	_, err := h(message.NewMessage("1", nil))

	assert.Equal(t, 2, runCount)
	assert.EqualError(t, err, "foo")
}

func TestRetry_retry_hook(t *testing.T) {
	var retriesFromHook []int

	retry := middleware.Retry{
		MaxRetries: 2,
		OnRetryHook: func(retryNum int, delay time.Duration) {
			retriesFromHook = append(retriesFromHook, retryNum)
		},
	}

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, errors.New("foo")
	})
	_, _ = h(message.NewMessage("1", nil))

	assert.EqualValues(t, []int{1, 2}, retriesFromHook)
}

func TestRetry_logger(t *testing.T) {
	logger := watermill.NewCaptureLogger()

	retry := middleware.Retry{
		MaxRetries: 2,
		Logger:     logger,
	}

	handlerErr := errors.New("foo")

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, handlerErr
	})
	_, _ = h(message.NewMessage("1", nil))

	assert.True(t, logger.HasError(handlerErr))
}
