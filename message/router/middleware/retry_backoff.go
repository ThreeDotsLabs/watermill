package middleware

import (
	"math/rand/v2"
	"time"
)

// exponentialBackoff computes the delays between retries.
//
// It computes the same delays as ExponentialBackOff from github.com/cenkalti/backoff/v5,
// which the Retry middleware used before, so existing configurations keep their waits.
// That includes zero values: with MaxInterval or Multiplier 0, every delay is InitialInterval.
type exponentialBackoff struct {
	initialInterval     time.Duration
	maxInterval         time.Duration
	multiplier          float64
	randomizationFactor float64

	// random returns a number from [0, 1).
	random func() float64

	// currentInterval is the delay before randomization; 0 means InitialInterval.
	currentInterval time.Duration
}

func newExponentialBackoff(r Retry) *exponentialBackoff {
	return &exponentialBackoff{
		initialInterval:     r.InitialInterval,
		maxInterval:         r.MaxInterval,
		multiplier:          r.Multiplier,
		randomizationFactor: r.RandomizationFactor,
		random:              rand.Float64,
	}
}

// next returns the delay before the next retry and advances the backoff.
// Each call returns the following delay, so it must be called once per retry.
func (b *exponentialBackoff) next() time.Duration {
	if b.currentInterval == 0 {
		b.currentInterval = b.initialInterval
	}

	delay := randomizeInterval(b.currentInterval, b.randomizationFactor, b.random())
	b.currentInterval = b.grow(b.currentInterval)

	return delay
}

// reset makes the next delay start again from initialInterval.
func (b *exponentialBackoff) reset() {
	b.currentInterval = 0
}

// grow returns interval multiplied by multiplier, but not more than maxInterval.
//
// With maxInterval 0, the result is 0. With multiplier 0, interval*multiplier is 0.
// next then starts again from initialInterval, so the delays do not grow.
func (b *exponentialBackoff) grow(interval time.Duration) time.Duration {
	// compared before multiplying, so that the multiplication cannot overflow
	if float64(interval) >= float64(b.maxInterval)/b.multiplier {
		return b.maxInterval
	}
	return time.Duration(float64(interval) * b.multiplier)
}

// randomizeInterval returns a value from [interval - factor*interval, interval + factor*interval],
// chosen by random, a number from [0, 1).
func randomizeInterval(interval time.Duration, factor float64, random float64) time.Duration {
	if factor == 0 {
		return interval
	}

	delta := factor * float64(interval)
	minInterval := float64(interval) - delta
	maxInterval := float64(interval) + delta

	// +1, so that maxInterval can be chosen as well
	return time.Duration(minInterval + random*(maxInterval-minInterval+1))
}
