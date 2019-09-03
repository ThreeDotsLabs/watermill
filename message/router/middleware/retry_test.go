package middleware_test

import (
	"context"
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
		Logger:     watermill.NewStdLogger(true, true),
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

func TestRetry_ctx_cancel(t *testing.T) {
	retry := middleware.Retry{
		InitialInterval: time.Minute,
	}

	producedMessages := message.Messages{message.NewMessage("2", nil)}

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		return producedMessages, errors.New("err")
	})

	msg := message.NewMessage("1", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	msg.SetContext(ctx)

	done := make(chan struct{})

	type handlerResult struct {
		Messages message.Messages
		Err      error
	}
	handlerResultCh := make(chan handlerResult, 1)

	go func() {
		messages, err := h(msg)
		handlerResultCh <- handlerResult{messages, err}
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("handler should be still during retrying")
	default:
		// ok
	}

	cancel()

	select {
	case <-done:
		// ok
	case <-time.After(time.Second):
		t.Fatal("ctx cancelled, retrying should be done")
	}

	handlerResultReceived := <-handlerResultCh

	assert.Error(t, handlerResultReceived.Err)
	assert.Equal(t, producedMessages, handlerResultReceived.Messages)
}

func TestRetry_max_elapsed(t *testing.T) {
	maxRetries := 10
	sleepInHandler := time.Millisecond * 20

	retry := middleware.Retry{
		MaxElapsedTime: time.Millisecond * 10,
		MaxRetries:     maxRetries,
	}

	runTimeWithoutMaxElapsedTime := sleepInHandler * time.Duration(maxRetries)

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		time.Sleep(sleepInHandler)
		return nil, errors.New("foo")
	})

	startTime := time.Now()
	_, _ = h(message.NewMessage("1", nil))
	timeElapsed := time.Since(startTime)

	assert.True(
		t,
		timeElapsed < runTimeWithoutMaxElapsedTime,
		"handler should run less than %s, time elapsed: %s",
		runTimeWithoutMaxElapsedTime,
		timeElapsed,
	)
}

func TestRetry_max_interval(t *testing.T) {
	maxRetries := 10
	backoffTimes := make([]time.Duration, maxRetries)
	maxInterval := time.Millisecond * 30

	retry := middleware.Retry{
		MaxRetries:          maxRetries,
		InitialInterval:     time.Millisecond * 10,
		MaxInterval:         maxInterval,
		Multiplier:          2.0,
		RandomizationFactor: 0,
		OnRetryHook: func(retryNum int, delay time.Duration) {
			backoffTimes[retryNum-1] = delay
		},
	}

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, errors.New("bar")
	})
	_, _ = h(message.NewMessage("2", nil))

	for i, delay := range backoffTimes {
		assert.True(t, delay <= maxInterval, "wait interval %d (%s) exceeds maxInterval (%s)", i, delay, maxInterval)
	}
}
