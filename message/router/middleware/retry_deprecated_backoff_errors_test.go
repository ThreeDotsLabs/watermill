package middleware_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

// PermanentError and RetryAfterError have the same names, fields and methods
// as in github.com/cenkalti/backoff/v5, which the tests do not import.

type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return e.Err.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.Err
}

type RetryAfterError struct {
	Duration time.Duration
}

func (e *RetryAfterError) Error() string {
	return fmt.Sprintf("retry after %s", e.Duration)
}

// useFakeDeprecatedBackoffPackage makes the Retry middleware treat the types above as the types of backoff v5.
func useFakeDeprecatedBackoffPackage(t *testing.T) {
	t.Cleanup(middleware.SetDeprecatedBackoffPkgPath(reflect.TypeOf(PermanentError{}).PkgPath()))
}

func loggedErrors(logger *watermill.CaptureLoggerAdapter, msg string) []watermill.CapturedMessage {
	var found []watermill.CapturedMessage
	for _, captured := range logger.Captured()[watermill.ErrorLogLevel] {
		if captured.Msg == msg {
			found = append(found, captured)
		}
	}
	return found
}

const (
	backoffPermanentDeprecatedMsg  = "Deprecation warning: handler returned backoff.PermanentError, which is deprecated in the Retry middleware, use middleware.Permanent instead"
	backoffRetryAfterDeprecatedMsg = "Deprecation warning: handler returned backoff.RetryAfterError, which is deprecated in the Retry middleware, use middleware.RetryAfter instead"
)

func TestRetry_backoff_permanent_error_still_stops_retries(t *testing.T) {
	useFakeDeprecatedBackoffPackage(t)

	handlerErr := errors.New("foo")
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
		return nil, &PermanentError{Err: handlerErr}
	})
	_, err := h(message.NewMessage("1", nil))

	// the same as middleware.Permanent
	assert.Equal(t, 2, runCount)
	assert.Same(t, handlerErr, err)
	assert.Equal(t, []int{1}, shouldRetryNums)
	assert.Empty(t, retryHookNums)
	assert.False(t, exhaustedCalled)
}

func TestRetry_wrapped_backoff_permanent_error_still_stops_retries(t *testing.T) {
	useFakeDeprecatedBackoffPackage(t)

	handlerErr := errors.New("foo")

	testCases := []struct {
		name string
		err  error
	}{
		{name: "wrapped", err: fmt.Errorf("handling failed: %w", &PermanentError{Err: handlerErr})},
		{name: "joined", err: errors.Join(errors.New("other"), &PermanentError{Err: handlerErr})},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runCount := 0

			retry := middleware.Retry{MaxRetries: 5}
			h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
				runCount++
				return nil, tc.err
			})
			_, err := h(message.NewMessage("1", nil))

			assert.Equal(t, 1, runCount)
			assert.Same(t, handlerErr, err)
		})
	}
}

// As with middleware.RetryAfter, the backoff then starts again from InitialInterval.
func TestRetry_backoff_retry_after_error_still_sets_the_delay(t *testing.T) {
	useFakeDeprecatedBackoffPackage(t)

	logger := watermill.NewCaptureLogger()
	var shouldRetryDelays []time.Duration

	retry := exactBackoffRetry()
	retry.Logger = logger
	retry.ShouldRetry = func(params middleware.RetryParams) bool {
		shouldRetryDelays = append(shouldRetryDelays, params.Delay)
		return true
	}

	runCount := 0
	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		switch runCount {
		case 3:
			return nil, &RetryAfterError{Duration: 3 * time.Millisecond}
		case 1, 2, 4:
			return nil, errors.New("foo")
		}
		return nil, nil
	})
	_, err := h(message.NewMessage("1", nil))
	require.NoError(t, err)

	expectedDelays := []time.Duration{
		2 * time.Millisecond,
		4 * time.Millisecond,
		3 * time.Millisecond, // from RetryAfterError
		2 * time.Millisecond, // InitialInterval again
	}
	assert.Equal(t, expectedDelays, shouldRetryDelays)
	assert.Equal(t, expectedDelays, loggedWaitTimes(logger))
}

func TestRetry_backoff_errors_log_deprecation_each_time(t *testing.T) {
	useFakeDeprecatedBackoffPackage(t)

	logger := watermill.NewCaptureLogger()
	retry := middleware.Retry{
		MaxRetries: 1,
		Logger:     logger,
	}

	permanentErr := &PermanentError{Err: errors.New("foo")}
	retryAfterErr := &RetryAfterError{Duration: time.Millisecond}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		if msg.UUID == "retry-after" {
			return nil, retryAfterErr
		}
		return nil, permanentErr
	})

	for _, uuid := range []string{"permanent", "permanent", "retry-after", "retry-after"} {
		_, err := h(message.NewMessage(uuid, nil))
		require.Error(t, err)
	}

	// a permanent error ends the retries, so each message logs it once
	permanentLogs := loggedErrors(logger, backoffPermanentDeprecatedMsg)
	require.Len(t, permanentLogs, 2)
	for _, log := range permanentLogs {
		assert.Same(t, permanentErr, log.Err)
	}

	// with MaxRetries 1, each message gets RetryAfterError twice: on the first attempt and on the retry
	retryAfterLogs := loggedErrors(logger, backoffRetryAfterDeprecatedMsg)
	require.Len(t, retryAfterLogs, 4)
	for _, log := range retryAfterLogs {
		assert.Same(t, retryAfterErr, log.Err)
	}
}

func TestRetry_backoff_errors_without_logger(t *testing.T) {
	useFakeDeprecatedBackoffPackage(t)

	handlerErr := errors.New("foo")
	retry := middleware.Retry{MaxRetries: 5}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		return nil, &PermanentError{Err: handlerErr}
	})
	_, err := h(message.NewMessage("1", nil))

	assert.Same(t, handlerErr, err)
}

// Only the types of backoff v5 are recognized, not types with the same names from other packages.
func TestRetry_errors_with_backoff_names_from_other_packages_are_retried(t *testing.T) {
	logger := watermill.NewCaptureLogger()
	runCount := 0

	retry := middleware.Retry{
		MaxRetries: 2,
		Logger:     logger,
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return nil, &PermanentError{Err: errors.New("foo")}
	})
	_, err := h(message.NewMessage("1", nil))

	var permanentErr *PermanentError
	assert.ErrorAs(t, err, &permanentErr)
	assert.Equal(t, 3, runCount)
	assert.Empty(t, loggedErrors(logger, backoffPermanentDeprecatedMsg))
}

type otherBackoffError struct{}

func (e *otherBackoffError) Error() string { return "other" }

func (e *otherBackoffError) Unwrap() error { return errors.New("wrapped by other") }

// Other types of the backoff package are retried as any error.
func TestRetry_other_backoff_errors_are_retried(t *testing.T) {
	useFakeDeprecatedBackoffPackage(t)

	runCount := 0
	retry := middleware.Retry{MaxRetries: 2}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return nil, &otherBackoffError{}
	})
	_, err := h(message.NewMessage("1", nil))

	var otherErr *otherBackoffError
	assert.ErrorAs(t, err, &otherErr)
	assert.Equal(t, 3, runCount)
}
