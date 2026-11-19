package middleware

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func nextDelays(b *exponentialBackoff, n int) []time.Duration {
	delays := make([]time.Duration, n)
	for i := range delays {
		delays[i] = b.next()
	}
	return delays
}

func TestExponentialBackoff_delays(t *testing.T) {
	const ms = time.Millisecond

	testCases := []struct {
		name     string
		retry    Retry
		expected []time.Duration
	}{
		{
			name:     "multiplied_by_multiplier",
			retry:    Retry{InitialInterval: 10 * ms, MaxInterval: time.Second, Multiplier: 2},
			expected: []time.Duration{10 * ms, 20 * ms, 40 * ms, 80 * ms, 160 * ms},
		},
		{
			name:     "fractional_multiplier",
			retry:    Retry{InitialInterval: 10 * ms, MaxInterval: time.Second, Multiplier: 1.5},
			expected: []time.Duration{10 * ms, 15 * ms, 22500 * time.Microsecond, 33750 * time.Microsecond},
		},
		{
			name:     "capped_at_max_interval",
			retry:    Retry{InitialInterval: 10 * ms, MaxInterval: 30 * ms, Multiplier: 2},
			expected: []time.Duration{10 * ms, 20 * ms, 30 * ms, 30 * ms, 30 * ms},
		},
		{
			name:     "max_interval_lower_than_initial_interval",
			retry:    Retry{InitialInterval: 10 * ms, MaxInterval: 5 * ms, Multiplier: 2},
			expected: []time.Duration{10 * ms, 5 * ms, 5 * ms},
		},
		{
			// as with backoff v5: an unset MaxInterval keeps every delay at InitialInterval
			name:     "zero_max_interval_does_not_grow",
			retry:    Retry{InitialInterval: 10 * ms, Multiplier: 2},
			expected: []time.Duration{10 * ms, 10 * ms, 10 * ms},
		},
		{
			name:     "zero_multiplier_does_not_grow",
			retry:    Retry{InitialInterval: 10 * ms, MaxInterval: time.Second},
			expected: []time.Duration{10 * ms, 10 * ms, 10 * ms},
		},
		{
			name:     "multiplier_1_does_not_grow",
			retry:    Retry{InitialInterval: 10 * ms, MaxInterval: time.Second, Multiplier: 1},
			expected: []time.Duration{10 * ms, 10 * ms, 10 * ms},
		},
		{
			name:     "multiplier_below_1_shrinks",
			retry:    Retry{InitialInterval: 40 * ms, MaxInterval: time.Second, Multiplier: 0.5},
			expected: []time.Duration{40 * ms, 20 * ms, 10 * ms},
		},
		{
			name:     "zero_initial_interval_does_not_wait",
			retry:    Retry{MaxInterval: time.Second, Multiplier: 2},
			expected: []time.Duration{0, 0, 0},
		},
		{
			name:     "zero_config_does_not_wait",
			retry:    Retry{},
			expected: []time.Duration{0, 0, 0},
		},
		{
			name: "does_not_overflow",
			retry: Retry{
				InitialInterval: time.Duration(math.MaxInt64 / 2),
				MaxInterval:     time.Duration(math.MaxInt64),
				Multiplier:      3,
			},
			expected: []time.Duration{
				time.Duration(math.MaxInt64 / 2),
				time.Duration(math.MaxInt64),
				time.Duration(math.MaxInt64),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := newExponentialBackoff(tc.retry)
			assert.Equal(t, tc.expected, nextDelays(b, len(tc.expected)))
		})
	}
}

func TestExponentialBackoff_randomization(t *testing.T) {
	const ms = time.Millisecond

	newBackoff := func(random float64) *exponentialBackoff {
		b := newExponentialBackoff(Retry{
			InitialInterval:     10 * ms,
			MaxInterval:         time.Second,
			Multiplier:          2,
			RandomizationFactor: 0.5,
		})
		b.random = func() float64 { return random }
		return b
	}

	// the interval before randomization is 10ms, 20ms, 40ms, whatever was drawn
	assert.Equal(t, []time.Duration{5 * ms, 10 * ms, 20 * ms}, nextDelays(newBackoff(0), 3), "lowest")
	assert.Equal(t, []time.Duration{10 * ms, 20 * ms, 40 * ms}, nextDelays(newBackoff(0.5), 3), "middle")
	assert.Equal(t, []time.Duration{15 * ms, 30 * ms, 60 * ms}, nextDelays(newBackoff(math.Nextafter(1, 0)), 3), "highest")
}

func TestExponentialBackoff_randomization_factor_1(t *testing.T) {
	b := newExponentialBackoff(Retry{
		InitialInterval:     10 * time.Millisecond,
		MaxInterval:         time.Second,
		Multiplier:          2,
		RandomizationFactor: 1,
	})
	b.random = func() float64 { return 0 }

	assert.Equal(t, []time.Duration{0, 0}, nextDelays(b, 2), "factor 1 can choose no wait")
}

func TestExponentialBackoff_random_delays_within_bounds(t *testing.T) {
	retry := Retry{
		InitialInterval:     10 * time.Millisecond,
		MaxInterval:         time.Second,
		Multiplier:          2,
		RandomizationFactor: 0.5,
	}
	b := newExponentialBackoff(retry)

	interval := retry.InitialInterval
	seen := map[time.Duration]bool{}

	for i := range 6 {
		delay := b.next()
		seen[delay] = true

		assert.GreaterOrEqual(t, delay, interval/2, "delay %d", i)
		assert.LessOrEqual(t, delay, interval*3/2, "delay %d", i)

		interval *= 2
	}

	assert.Len(t, seen, 6)
}

func TestExponentialBackoff_each_instance_starts_from_initial_interval(t *testing.T) {
	retry := Retry{InitialInterval: time.Millisecond, MaxInterval: time.Second, Multiplier: 2}

	first := newExponentialBackoff(retry)
	_ = nextDelays(first, 5)

	second := newExponentialBackoff(retry)
	assert.Equal(t, time.Millisecond, second.next())
}

func TestExponentialBackoff_reset(t *testing.T) {
	b := newExponentialBackoff(Retry{InitialInterval: time.Millisecond, MaxInterval: time.Second, Multiplier: 2})

	assert.Equal(t, []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond}, nextDelays(b, 3))
	b.reset()
	assert.Equal(t, []time.Duration{time.Millisecond, 2 * time.Millisecond}, nextDelays(b, 2))
}
