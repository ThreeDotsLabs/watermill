package middleware_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

func TestPermanent(t *testing.T) {
	err := errors.New("foo")

	permanentErr := middleware.Permanent(err)

	assert.ErrorIs(t, permanentErr, err)
	assert.Equal(t, "foo", permanentErr.Error())

	var target *middleware.PermanentError
	require.ErrorAs(t, permanentErr, &target)
	assert.Same(t, err, target.Err)
}

func TestPermanent_nil(t *testing.T) {
	assert.NoError(t, middleware.Permanent(nil))
}

func TestRetry_permanent_error_is_not_retried(t *testing.T) {
	handlerErr := errors.New("foo")
	producedMessages := message.Messages{message.NewMessage("2", nil)}
	var shouldRetryNums, retryHookNums []int
	exhaustedCalled := false
	runCount := 0

	retry := middleware.Retry{
		MaxRetries: 5,
		ShouldRetry: func(params middleware.RetryParams) bool {
			shouldRetryNums = append(shouldRetryNums, params.RetryNum)
			return true
		},
		OnRetryHook: func(retryNum int, delay time.Duration) {
			retryHookNums = append(retryHookNums, retryNum)
		},
		OnRetriesExhausted: func(params middleware.RetriesExhaustedParams) {
			exhaustedCalled = true
		},
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		if runCount == 1 {
			return nil, errors.New("not permanent")
		}
		return producedMessages, middleware.Permanent(handlerErr)
	})
	handlerMessages, err := h(message.NewMessage("1", nil))

	assert.Equal(t, 2, runCount)
	assert.EqualValues(t, producedMessages, handlerMessages)
	// the error is returned without the PermanentError around it
	assert.Same(t, handlerErr, err)
	// the permanent error ends the retries, so the hooks are not told about a retry
	assert.Equal(t, []int{1}, shouldRetryNums)
	assert.Empty(t, retryHookNums)
	assert.False(t, exhaustedCalled)
}

func TestRetry_wrapped_permanent_error_is_not_retried(t *testing.T) {
	handlerErr := errors.New("foo")
	runCount := 0

	retry := middleware.Retry{
		MaxRetries: 5,
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return nil, fmt.Errorf("handling failed: %w", middleware.Permanent(handlerErr))
	})
	_, err := h(message.NewMessage("1", nil))

	assert.Equal(t, 1, runCount)
	assert.Same(t, handlerErr, err)
}

func TestRetryAfter(t *testing.T) {
	err := errors.New("foo")

	retryAfterErr := middleware.RetryAfter(err, time.Second)

	assert.ErrorIs(t, retryAfterErr, err)
	assert.Equal(t, "foo", retryAfterErr.Error())

	var target *middleware.RetryAfterError
	require.ErrorAs(t, retryAfterErr, &target)
	assert.Same(t, err, target.Err)
	assert.Equal(t, time.Second, target.Delay)
}

func TestRetryAfter_nil(t *testing.T) {
	assert.NoError(t, middleware.RetryAfter(nil, time.Second))
}

// The delay from RetryAfter is waited instead of the next backoff delay,
// and the backoff then starts again from InitialInterval.
func TestRetry_retry_after_sets_the_delay(t *testing.T) {
	logger := watermill.NewCaptureLogger()
	var shouldRetryDelays []time.Duration
	var shouldRetryErrs []error
	retryAfterCause := errors.New("rate limited")

	retry := exactBackoffRetry()
	retry.Logger = logger
	retry.ShouldRetry = func(params middleware.RetryParams) bool {
		shouldRetryDelays = append(shouldRetryDelays, params.Delay)
		shouldRetryErrs = append(shouldRetryErrs, params.Err)
		return true
	}

	runCount := 0
	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		switch runCount {
		case 3:
			return nil, middleware.RetryAfter(retryAfterCause, 3*time.Millisecond)
		case 1, 2, 4:
			return nil, errors.New("foo")
		}
		return nil, nil
	})
	_, err := h(message.NewMessage("1", nil))
	require.NoError(t, err)

	expectedWaits := []time.Duration{
		2 * time.Millisecond,
		4 * time.Millisecond,
		3 * time.Millisecond, // from RetryAfter
		2 * time.Millisecond, // InitialInterval again
	}
	assert.Equal(t, expectedWaits, loggedWaitTimes(logger))
	assert.Equal(t, expectedWaits, shouldRetryDelays)
	assert.ErrorIs(t, shouldRetryErrs[2], retryAfterCause)
}

func TestRetry_retry_after_respects_max_elapsed_time(t *testing.T) {
	retryAfterErr := middleware.RetryAfter(errors.New("foo"), time.Minute)
	runCount := 0

	retry := middleware.Retry{
		MaxRetries:     5,
		MaxElapsedTime: 50 * time.Millisecond,
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return nil, retryAfterErr
	})

	start := time.Now()
	_, err := h(message.NewMessage("1", nil))

	// a minute does not fit in MaxElapsedTime, so the middleware does not wait it
	assert.Less(t, time.Since(start), time.Second)
	assert.Equal(t, 1, runCount)
	// unlike a permanent error, the error is returned as the handler returned it
	assert.Same(t, retryAfterErr, err)
}

func TestRetry_retry_after_respects_max_retries(t *testing.T) {
	runCount := 0

	retry := middleware.Retry{
		MaxRetries: 2,
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return nil, middleware.RetryAfter(errors.New("foo"), time.Millisecond)
	})
	_, err := h(message.NewMessage("1", nil))

	require.Error(t, err)
	assert.Equal(t, 3, runCount)
}
