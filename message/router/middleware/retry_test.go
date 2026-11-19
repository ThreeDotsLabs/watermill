package middleware_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
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
		if runCount == 1 {
			return nil, errors.New("foo")
		}

		return producedMessages, nil
	})

	handlerMessages, handlerErr := h(message.NewMessage("1", nil))

	assert.Equal(t, 2, runCount)
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

func TestRetry_retries_exhausted_hook(t *testing.T) {
	var exhaustedParams middleware.RetriesExhaustedParams

	handlerErr := errors.New("foo")

	retry := middleware.Retry{
		MaxRetries: 2,
		OnRetriesExhausted: func(params middleware.RetriesExhaustedParams) {
			exhaustedParams = params
		},
	}

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, handlerErr
	})
	_, _ = h(message.NewMessage("1", nil))

	assert.ErrorIs(t, exhaustedParams.Err, handlerErr)
	// With MaxRetries=2, there are 3 total attempts (1 initial + 2 retries)
	// retryNum is incremented after each attempt, so it's 3 when exhausted
	assert.Equal(t, 3, exhaustedParams.RetryNum)
}

func TestRetry_retries_exhausted_hook_not_called_on_success(t *testing.T) {
	var hookCalled bool

	retry := middleware.Retry{
		MaxRetries: 2,
		OnRetriesExhausted: func(params middleware.RetriesExhaustedParams) {
			hookCalled = true
		},
	}

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, nil
	})
	_, _ = h(message.NewMessage("1", nil))

	assert.False(t, hookCalled, "OnRetriesExhausted should not be called on success")
}

func TestRetry_retries_exhausted_hook_not_called_when_should_retry_returns_false(t *testing.T) {
	var hookCalled bool

	retry := middleware.Retry{
		MaxRetries: 5,
		ShouldRetry: func(params middleware.RetryParams) bool {
			return false
		},
		OnRetriesExhausted: func(params middleware.RetriesExhaustedParams) {
			hookCalled = true
		},
	}

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, errors.New("foo")
	})
	_, _ = h(message.NewMessage("1", nil))

	assert.False(t, hookCalled, "OnRetriesExhausted should not be called when ShouldRetry returns false")
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
		MaxRetries:      1,
		InitialInterval: time.Minute,
	}

	producedMessages := message.Messages{message.NewMessage("2", nil)}

	runCount := 0
	firstAttemptDone := make(chan struct{})

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		runCount++
		if runCount == 1 {
			close(firstAttemptDone)
		}
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
	case <-firstAttemptDone:
		// cancelling before the first attempt finished would not test cancelling the wait before the retry
	case <-time.After(10 * time.Second):
		t.Fatal("handler was not called")
	}

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
	case <-time.After(10 * time.Second):
		t.Fatal("ctx cancelled, retrying should be done")
	}

	handlerResultReceived := <-handlerResultCh

	assert.Equal(t, 1, runCount, "handler should not be retried after ctx is cancelled")
	assert.ErrorIs(t, handlerResultReceived.Err, context.Canceled)
	// the pre-v5 assertion; nil passed only when cancel() won the race against the first attempt
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
	t.Parallel()

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

func TestRetry_first_run_no_delay(t *testing.T) {
	t.Parallel()

	initialInterval := time.Millisecond * 100
	retry := middleware.Retry{
		MaxElapsedTime:  initialInterval * 2,
		MaxRetries:      10,
		InitialInterval: initialInterval,
	}

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, nil
	})

	start := time.Now()
	_, _ = h(message.NewMessage("1", nil))
	elapsed := time.Since(start)

	assert.True(t, elapsed < initialInterval, "first retry should not wait, elapsed: %s", elapsed)
}

func TestRetry_should_retry(t *testing.T) {
	errToSkip := errors.New("this should be skipped")

	retry := middleware.Retry{
		MaxRetries: 5,
		ShouldRetry: func(params middleware.RetryParams) bool {
			return !errors.Is(params.Err, errToSkip)
		},
	}

	runCount := 0

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		runCount++
		return nil, errToSkip
	})

	handlerMessages, handlerErr := h(message.NewMessage("1", nil))

	assert.Equal(t, 1, runCount)
	assert.Nil(t, handlerMessages)
	// the handler's error is returned as it is, not wrapped
	assert.Same(t, errToSkip, handlerErr)
}

// Test that the ShouldRetry function is called on each retry attempt.
// Under the hood, the second attempt goes over a bit different code path,
// so we want to make sure that it works consistently.
func TestRetry_should_retry_second_attempt(t *testing.T) {
	errToSkip := errors.New("this should be skipped")

	retry := middleware.Retry{
		MaxRetries: 5,
		ShouldRetry: func(params middleware.RetryParams) bool {
			return !errors.Is(params.Err, errToSkip)
		},
	}

	runCount := 0

	h := retry.Middleware(func(msg *message.Message) (messages []*message.Message, e error) {
		runCount++

		if runCount == 1 {
			return nil, errors.New("some other error")
		} else {
			return nil, errToSkip
		}
	})

	handlerMessages, handlerErr := h(message.NewMessage("1", nil))

	assert.Equal(t, 2, runCount)
	assert.Nil(t, handlerMessages)
	// the handler's error is returned as it is, not wrapped
	assert.Same(t, errToSkip, handlerErr)
}

// TestRetry_uncancel_context checks scenario when context is canceled,
// and we want to retry. More context: https://github.com/ThreeDotsLabs/watermill/issues/467
//
// Message is passed as a pointer, so underlying middlewares or handlers can cancel its context.
// In this scenario, retrying is pointless because context is already canceled, so any operation
// that relies on context will fail immediately.
func TestRetry_uncancel_context(t *testing.T) {
	retry := middleware.Retry{
		MaxRetries:          5,
		ResetContextOnRetry: true,
	}

	num := 0

	var ctxCancelMiddleware = func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			num++

			ctx, cancel := context.WithCancel(msg.Context())
			defer func() {
				cancel()
			}()

			if num == 1 {
				t.Log("Run 1: canceling context")
				cancel()
			} else {
				t.Logf("Run %d: context is not canceled", num)
			}

			msg.SetContext(ctx)
			return h(msg)
		}
	}

	h := func(msg *message.Message) (messages []*message.Message, e error) {
		return nil, msg.Context().Err()
	}

	h = ctxCancelMiddleware(h)
	h = retry.Middleware(h)

	_, handlerErr := h(message.NewMessage("1", nil))
	assert.NoError(t, handlerErr)
}

// No randomization, so the waits can be compared exactly with exactBackoffWaits.
func exactBackoffRetry() middleware.Retry {
	return middleware.Retry{
		MaxRetries:          5,
		InitialInterval:     2 * time.Millisecond,
		Multiplier:          2,
		MaxInterval:         time.Second,
		RandomizationFactor: 0,
	}
}

var exactBackoffWaits = []time.Duration{
	2 * time.Millisecond,
	4 * time.Millisecond,
	8 * time.Millisecond,
}

func failingHandler(failures int) message.HandlerFunc {
	calls := 0
	return func(msg *message.Message) ([]*message.Message, error) {
		calls++
		if calls <= failures {
			return nil, errors.New("foo")
		}
		return nil, nil
	}
}

// wait_time is logged right before the middleware waits, with the delay it then waits,
// so these are the real waits, not the delays the hooks were given.
func loggedWaitTimes(logger *watermill.CaptureLoggerAdapter) []time.Duration {
	var waits []time.Duration
	for _, msg := range logger.Captured()[watermill.ErrorLogLevel] {
		if msg.Msg != "Error occurred, retrying" {
			continue
		}
		waits = append(waits, msg.Fields["wait_time"].(time.Duration))
	}
	return waits
}

func TestRetry_should_retry_retry_num_starts_from_1(t *testing.T) {
	var retryNums []int

	retry := middleware.Retry{
		MaxRetries: 5,
		ShouldRetry: func(params middleware.RetryParams) bool {
			retryNums = append(retryNums, params.RetryNum)
			return true
		},
	}

	h := retry.Middleware(failingHandler(3))
	_, err := h(message.NewMessage("1", nil))
	require.NoError(t, err)

	assert.Equal(t, []int{1, 2, 3}, retryNums)
}

// Guards against hooks reading the delay with NextBackOff, which advanced the backoff and made the real waits skip intervals.
func TestRetry_hooks_do_not_change_wait_times(t *testing.T) {
	alwaysRetry := func(middleware.RetryParams) bool { return true }
	noopRetryHook := func(int, time.Duration) {}

	testCases := []struct {
		name        string
		shouldRetry func(middleware.RetryParams) bool
		onRetryHook func(int, time.Duration)
	}{
		{name: "no_hooks"},
		{name: "should_retry", shouldRetry: alwaysRetry},
		{name: "on_retry_hook", onRetryHook: noopRetryHook},
		{name: "both_hooks", shouldRetry: alwaysRetry, onRetryHook: noopRetryHook},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger := watermill.NewCaptureLogger()

			retry := exactBackoffRetry()
			retry.Logger = logger
			retry.ShouldRetry = tc.shouldRetry
			retry.OnRetryHook = tc.onRetryHook

			h := retry.Middleware(failingHandler(3))
			_, err := h(message.NewMessage("1", nil))
			require.NoError(t, err)

			assert.Equal(t, exactBackoffWaits, loggedWaitTimes(logger))
		})
	}
}

func TestRetry_should_retry_gets_real_delay(t *testing.T) {
	logger := watermill.NewCaptureLogger()
	var delays []time.Duration

	retry := exactBackoffRetry()
	retry.Logger = logger
	retry.ShouldRetry = func(params middleware.RetryParams) bool {
		delays = append(delays, params.Delay)
		return true
	}

	h := retry.Middleware(failingHandler(3))
	_, err := h(message.NewMessage("1", nil))
	require.NoError(t, err)

	assert.Equal(t, loggedWaitTimes(logger), delays)
}

func TestRetry_retry_hook_gets_real_delay(t *testing.T) {
	logger := watermill.NewCaptureLogger()
	var delays []time.Duration

	retry := exactBackoffRetry()
	retry.Logger = logger
	retry.OnRetryHook = func(retryNum int, delay time.Duration) {
		delays = append(delays, delay)
	}

	h := retry.Middleware(failingHandler(3))
	_, err := h(message.NewMessage("1", nil))
	require.NoError(t, err)

	waits := loggedWaitTimes(logger)
	require.Len(t, waits, 3)

	// OnRetryHook is called only after a failed retry, and retry 3 succeeds.
	assert.Equal(t, waits[:2], delays)
}

// The other tests compare with the logged waits; this one checks them against the clock.
// Only the lower bound is checked: timers never fire early, but on a busy machine they may fire late.
func TestRetry_waits_at_least_delay_given_to_hooks(t *testing.T) {
	var shouldRetryDelays, retryHookDelays []time.Duration

	retry := exactBackoffRetry()
	// longer than in other tests, so that scheduling latency is less likely to hide a too short wait
	retry.InitialInterval = 10 * time.Millisecond
	retry.ShouldRetry = func(params middleware.RetryParams) bool {
		shouldRetryDelays = append(shouldRetryDelays, params.Delay)
		return true
	}
	retry.OnRetryHook = func(retryNum int, delay time.Duration) {
		retryHookDelays = append(retryHookDelays, delay)
	}

	var attemptTimes []time.Time
	handler := failingHandler(3)

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		attemptTimes = append(attemptTimes, time.Now())
		return handler(msg)
	})
	_, err := h(message.NewMessage("1", nil))
	require.NoError(t, err)

	require.Len(t, attemptTimes, 4)
	require.Len(t, shouldRetryDelays, 3)
	require.Len(t, retryHookDelays, 2)

	for i := range shouldRetryDelays {
		wait := attemptTimes[i+1].Sub(attemptTimes[i])
		assert.GreaterOrEqual(t, wait, shouldRetryDelays[i], "ShouldRetry delay of retry %d", i+1)
		if i < len(retryHookDelays) {
			assert.GreaterOrEqual(t, wait, retryHookDelays[i], "OnRetryHook delay of retry %d", i+1)
		}
	}
}

func TestRetry_should_retry_not_called_when_retries_exhausted(t *testing.T) {
	var retryNums []int
	var exhaustedParams *middleware.RetriesExhaustedParams

	retry := middleware.Retry{
		MaxRetries: 2,
		ShouldRetry: func(params middleware.RetryParams) bool {
			retryNums = append(retryNums, params.RetryNum)
			return true
		},
		OnRetriesExhausted: func(params middleware.RetriesExhaustedParams) {
			exhaustedParams = &params
		},
	}

	h := retry.Middleware(failingHandler(10))
	_, err := h(message.NewMessage("1", nil))
	require.Error(t, err)

	// there is no retry 3, so ShouldRetry is not asked about it
	assert.Equal(t, []int{1, 2}, retryNums)
	require.NotNil(t, exhaustedParams)
	assert.Equal(t, 3, exhaustedParams.RetryNum)
}

func TestRetry_should_retry_not_called_when_max_elapsed_time_reached(t *testing.T) {
	shouldRetryCalled := false
	exhaustedCalled := false
	runCount := 0

	retry := middleware.Retry{
		MaxRetries:      5,
		InitialInterval: 50 * time.Millisecond,
		MaxElapsedTime:  10 * time.Millisecond,
		ShouldRetry: func(params middleware.RetryParams) bool {
			shouldRetryCalled = true
			return true
		},
		OnRetriesExhausted: func(params middleware.RetriesExhaustedParams) {
			exhaustedCalled = true
		},
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return nil, errors.New("foo")
	})
	_, err := h(message.NewMessage("1", nil))
	require.Error(t, err)

	// the first delay alone exceeds MaxElapsedTime, so there is no retry to ask about
	assert.Equal(t, 1, runCount)
	assert.False(t, shouldRetryCalled)
	assert.True(t, exhaustedCalled)
}

func TestRetry_no_retry_when_handler_succeeds(t *testing.T) {
	logger := watermill.NewCaptureLogger()
	hookCalled := false
	producedMessages := message.Messages{message.NewMessage("2", nil)}
	runCount := 0

	retry := middleware.Retry{
		MaxRetries:         5,
		InitialInterval:    time.Minute,
		Logger:             logger,
		ShouldRetry:        func(middleware.RetryParams) bool { hookCalled = true; return true },
		OnRetryHook:        func(int, time.Duration) { hookCalled = true },
		OnRetriesExhausted: func(middleware.RetriesExhaustedParams) { hookCalled = true },
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return producedMessages, nil
	})
	handlerMessages, err := h(message.NewMessage("1", nil))

	require.NoError(t, err)
	assert.EqualValues(t, producedMessages, handlerMessages)
	assert.Equal(t, 1, runCount)
	assert.False(t, hookCalled)
	assert.Empty(t, logger.Captured())
}

func TestRetry_max_retries_0_does_not_retry(t *testing.T) {
	shouldRetryCalled := false
	var exhaustedParams *middleware.RetriesExhaustedParams
	handlerErr := errors.New("foo")
	runCount := 0

	retry := middleware.Retry{
		MaxRetries:  0,
		ShouldRetry: func(middleware.RetryParams) bool { shouldRetryCalled = true; return true },
		OnRetriesExhausted: func(params middleware.RetriesExhaustedParams) {
			exhaustedParams = &params
		},
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return nil, handlerErr
	})
	_, err := h(message.NewMessage("1", nil))

	assert.Same(t, handlerErr, err)
	assert.Equal(t, 1, runCount)
	assert.False(t, shouldRetryCalled)
	require.NotNil(t, exhaustedParams)
	assert.Equal(t, 1, exhaustedParams.RetryNum)
}

func TestRetry_negative_max_retries_does_not_limit_retries(t *testing.T) {
	runCount := 0

	retry := middleware.Retry{
		MaxRetries: -1,
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		if runCount <= 50 {
			return nil, errors.New("foo")
		}
		return nil, nil
	})
	_, err := h(message.NewMessage("1", nil))

	require.NoError(t, err)
	assert.Equal(t, 51, runCount)
}

func TestRetry_returns_result_of_last_attempt_when_retries_exhausted(t *testing.T) {
	var lastMessages message.Messages
	var lastErr error
	var exhaustedParams *middleware.RetriesExhaustedParams
	runCount := 0

	retry := middleware.Retry{
		MaxRetries: 2,
		OnRetriesExhausted: func(params middleware.RetriesExhaustedParams) {
			exhaustedParams = &params
		},
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		lastMessages = message.Messages{message.NewMessage(fmt.Sprintf("produced-%d", runCount), nil)}
		lastErr = fmt.Errorf("error of attempt %d", runCount)
		return lastMessages, lastErr
	})
	handlerMessages, err := h(message.NewMessage("1", nil))

	assert.Equal(t, 3, runCount)
	assert.EqualValues(t, lastMessages, handlerMessages)
	assert.Same(t, lastErr, err)
	require.NotNil(t, exhaustedParams)
	assert.Same(t, lastErr, exhaustedParams.Err)
	assert.Equal(t, 3, exhaustedParams.RetryNum)
}

func TestRetry_should_retry_false_returns_result_of_attempt_without_waiting(t *testing.T) {
	producedMessages := message.Messages{message.NewMessage("2", nil)}
	handlerErr := errors.New("foo")
	runCount := 0

	retry := middleware.Retry{
		MaxRetries:      5,
		InitialInterval: time.Minute,
		ShouldRetry:     func(middleware.RetryParams) bool { return false },
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return producedMessages, handlerErr
	})

	start := time.Now()
	handlerMessages, err := h(message.NewMessage("1", nil))

	assert.Less(t, time.Since(start), time.Second)
	assert.Equal(t, 1, runCount)
	assert.EqualValues(t, producedMessages, handlerMessages)
	assert.Same(t, handlerErr, err)
}

func TestRetry_should_retry_gets_error_of_each_attempt(t *testing.T) {
	var handlerErrs, shouldRetryErrs []error
	runCount := 0

	retry := middleware.Retry{
		MaxRetries: 5,
		ShouldRetry: func(params middleware.RetryParams) bool {
			shouldRetryErrs = append(shouldRetryErrs, params.Err)
			return true
		},
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		if runCount <= 3 {
			err := fmt.Errorf("error of attempt %d", runCount)
			handlerErrs = append(handlerErrs, err)
			return nil, err
		}
		return nil, nil
	})
	_, err := h(message.NewMessage("1", nil))

	require.NoError(t, err)
	assert.Equal(t, handlerErrs, shouldRetryErrs)
}

func TestRetry_logs_each_retry(t *testing.T) {
	logger := watermill.NewCaptureLogger()

	retry := exactBackoffRetry()
	retry.Logger = logger

	var handlerErrs []error
	runCount := 0

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		if runCount <= 3 {
			err := fmt.Errorf("error of attempt %d", runCount)
			handlerErrs = append(handlerErrs, err)
			return nil, err
		}
		return nil, nil
	})
	_, err := h(message.NewMessage("1", nil))
	require.NoError(t, err)

	logged := logger.Captured()[watermill.ErrorLogLevel]
	require.Len(t, logged, 3)

	for i, msg := range logged {
		assert.Equal(t, "Error occurred, retrying", msg.Msg)
		assert.Same(t, handlerErrs[i], msg.Err)
		assert.Equal(t, watermill.LogFields{
			"retry_no":    i + 1,
			"max_retries": retry.MaxRetries,
			"wait_time":   exactBackoffWaits[i],
		}, msg.Fields)
	}
}

func TestRetry_does_not_log_when_not_retrying(t *testing.T) {
	logger := watermill.NewCaptureLogger()

	retry := middleware.Retry{
		MaxRetries: 0,
		Logger:     logger,
	}

	h := retry.Middleware(failingHandler(1))
	_, err := h(message.NewMessage("1", nil))
	require.Error(t, err)

	assert.Empty(t, logger.Captured())
}

func TestRetry_ctx_canceled_before_first_attempt(t *testing.T) {
	runCount := 0

	retry := middleware.Retry{
		MaxRetries: 5,
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return nil, nil
	})

	msg := message.NewMessage("1", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg.SetContext(ctx)

	_, err := h(msg)

	assert.Equal(t, 0, runCount)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestRetry_ctx_cancel_returns_cause(t *testing.T) {
	cause := errors.New("shutting down")
	firstAttemptDone := make(chan struct{})
	runCount := 0

	retry := middleware.Retry{
		MaxRetries:      1,
		InitialInterval: time.Minute,
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		if runCount == 1 {
			close(firstAttemptDone)
		}
		return nil, errors.New("foo")
	})

	msg := message.NewMessage("1", nil)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	msg.SetContext(ctx)

	errCh := make(chan error, 1)
	go func() {
		_, err := h(msg)
		errCh <- err
	}()

	<-firstAttemptDone
	cancel(cause)

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, cause)
	case <-time.After(10 * time.Second):
		t.Fatal("ctx canceled, retrying should be done")
	}
	assert.Equal(t, 1, runCount)
}

func TestRetry_waits_follow_backoff_config(t *testing.T) {
	const ms = time.Millisecond

	testCases := []struct {
		name          string
		retry         middleware.Retry
		expectedWaits []time.Duration
	}{
		{
			name:          "multiplied_by_multiplier",
			retry:         middleware.Retry{InitialInterval: 2 * ms, MaxInterval: time.Second, Multiplier: 2},
			expectedWaits: []time.Duration{2 * ms, 4 * ms, 8 * ms, 16 * ms},
		},
		{
			name:          "capped_at_max_interval",
			retry:         middleware.Retry{InitialInterval: 2 * ms, MaxInterval: 5 * ms, Multiplier: 2},
			expectedWaits: []time.Duration{2 * ms, 4 * ms, 5 * ms, 5 * ms},
		},
		{
			name:          "zero_max_interval_does_not_grow",
			retry:         middleware.Retry{InitialInterval: 2 * ms, Multiplier: 2},
			expectedWaits: []time.Duration{2 * ms, 2 * ms, 2 * ms, 2 * ms},
		},
		{
			name:          "zero_multiplier_does_not_grow",
			retry:         middleware.Retry{InitialInterval: 2 * ms, MaxInterval: time.Second},
			expectedWaits: []time.Duration{2 * ms, 2 * ms, 2 * ms, 2 * ms},
		},
		{
			name:          "zero_initial_interval_does_not_wait",
			retry:         middleware.Retry{MaxInterval: time.Second, Multiplier: 2},
			expectedWaits: []time.Duration{0, 0, 0, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger := watermill.NewCaptureLogger()

			retry := tc.retry
			retry.MaxRetries = 10
			retry.Logger = logger

			h := retry.Middleware(failingHandler(len(tc.expectedWaits)))
			_, err := h(message.NewMessage("1", nil))
			require.NoError(t, err)

			assert.Equal(t, tc.expectedWaits, loggedWaitTimes(logger))
		})
	}
}

func TestRetry_randomized_delay_given_to_hooks_is_waited(t *testing.T) {
	logger := watermill.NewCaptureLogger()
	var shouldRetryDelays, retryHookDelays []time.Duration

	retry := exactBackoffRetry()
	retry.RandomizationFactor = 0.5
	retry.Logger = logger
	retry.ShouldRetry = func(params middleware.RetryParams) bool {
		shouldRetryDelays = append(shouldRetryDelays, params.Delay)
		return true
	}
	retry.OnRetryHook = func(retryNum int, delay time.Duration) {
		retryHookDelays = append(retryHookDelays, delay)
	}

	h := retry.Middleware(failingHandler(3))
	_, err := h(message.NewMessage("1", nil))
	require.NoError(t, err)

	waits := loggedWaitTimes(logger)
	require.Len(t, waits, 3)

	assert.Equal(t, waits, shouldRetryDelays)
	assert.Equal(t, waits[:2], retryHookDelays)

	for i, wait := range waits {
		assert.GreaterOrEqual(t, wait, exactBackoffWaits[i]/2, "wait %d", i)
		assert.LessOrEqual(t, wait, exactBackoffWaits[i]*3/2, "wait %d", i)
	}
}

func TestRetry_max_elapsed_time_includes_next_delay(t *testing.T) {
	var shouldRetryDelays []time.Duration
	runCount := 0

	retry := middleware.Retry{
		MaxRetries:      5,
		InitialInterval: 20 * time.Millisecond,
		MaxInterval:     time.Second,
		Multiplier:      2,
		MaxElapsedTime:  50 * time.Millisecond,
		ShouldRetry: func(params middleware.RetryParams) bool {
			shouldRetryDelays = append(shouldRetryDelays, params.Delay)
			return true
		},
	}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		runCount++
		return nil, errors.New("foo")
	})
	_, err := h(message.NewMessage("1", nil))
	require.Error(t, err)

	// waiting 20ms fits in 50ms, but 20ms elapsed + the next 40ms would not
	assert.Equal(t, 2, runCount)
	assert.Equal(t, []time.Duration{20 * time.Millisecond}, shouldRetryDelays)
}

func TestRetry_each_message_starts_from_initial_interval(t *testing.T) {
	retry := exactBackoffRetry()

	delays := map[string][]time.Duration{}
	var currentMessage string
	retry.ShouldRetry = func(params middleware.RetryParams) bool {
		delays[currentMessage] = append(delays[currentMessage], params.Delay)
		return true
	}

	attempts := map[string]int{}
	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		currentMessage = msg.UUID
		attempts[msg.UUID]++
		if attempts[msg.UUID] <= 2 {
			return nil, errors.New("foo")
		}
		return nil, nil
	})

	// the same handler for both messages, so that a backoff shared between messages would show
	for _, uuid := range []string{"1", "2"} {
		_, err := h(message.NewMessage(uuid, nil))
		require.NoError(t, err)
	}

	assert.Equal(t, exactBackoffWaits[:2], delays["1"])
	assert.Equal(t, exactBackoffWaits[:2], delays["2"])
}

func TestRetry_messages_handled_concurrently(t *testing.T) {
	retry := exactBackoffRetry()

	var lock sync.Mutex
	attempts := map[string]int{}

	h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
		lock.Lock()
		defer lock.Unlock()

		attempts[msg.UUID]++
		if attempts[msg.UUID] <= 2 {
			return nil, errors.New("foo")
		}
		return nil, nil
	})

	const messagesCount = 20
	var wg sync.WaitGroup
	errs := make(chan error, messagesCount)

	for i := range messagesCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h(message.NewMessage(fmt.Sprintf("%d", i), nil))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		assert.NoError(t, err)
	}
	assert.Len(t, attempts, messagesCount)
	for uuid, n := range attempts {
		assert.Equal(t, 3, n, "attempts of message %s", uuid)
	}
}

type retryTestContextKey struct{}

func TestRetry_reset_context_on_retry(t *testing.T) {
	testCases := []struct {
		resetContextOnRetry bool
		expectedValue       any
	}{
		{resetContextOnRetry: false, expectedValue: "set by attempt 1"},
		{resetContextOnRetry: true, expectedValue: nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("reset_%t", tc.resetContextOnRetry), func(t *testing.T) {
			retry := middleware.Retry{
				MaxRetries:          1,
				ResetContextOnRetry: tc.resetContextOnRetry,
			}

			runCount := 0
			var valueInRetry any

			h := retry.Middleware(func(msg *message.Message) ([]*message.Message, error) {
				runCount++
				if runCount == 1 {
					msg.SetContext(context.WithValue(msg.Context(), retryTestContextKey{}, "set by attempt 1"))
					return nil, errors.New("foo")
				}
				valueInRetry = msg.Context().Value(retryTestContextKey{})
				return nil, nil
			})

			_, err := h(message.NewMessage("1", nil))
			require.NoError(t, err)

			assert.Equal(t, tc.expectedValue, valueInRetry)
		})
	}
}
