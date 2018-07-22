package middleware

import (
	"github.com/roblaszczak/gooddd/message"
	"github.com/throttled/throttled"
	"time"
	"github.com/roblaszczak/gooddd"
	"github.com/throttled/throttled/store/memstore"
	"github.com/pkg/errors"
)

type Throttle struct {
	rateLimiter throttled.RateLimiter
	logger      gooddd.LoggerAdapter
}

func NewThrottlePerSecond(perSecond int, logger gooddd.LoggerAdapter) (Throttle, error) {
	store, err := memstore.New(1)
	if err != nil {
		return Throttle{}, errors.Wrap(err, "cannot create throttle storage")
	}

	quota := throttled.RateQuota{throttled.PerSec(perSecond), 0}
	rateLimiter, err := throttled.NewGCRARateLimiter(store, quota)
	if err != nil {
		return Throttle{}, errors.Wrap(err, "cannot initialize rate limiter")
	}

	return Throttle{rateLimiter, logger}, nil
}

func (t Throttle) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(message message.ConsumedMessage) ([]message.ProducedMessage, error) {
		defer func() {
			limited, context, err := t.rateLimiter.RateLimit("", 1)
			if err != nil {
				t.logger.Info("Throttle error", gooddd.LogFields{"err": err.Error()})
			}
			if limited {
				t.logger.Info("Throttling", gooddd.LogFields{"wait_duration": context.RetryAfter.String()})
				time.Sleep(context.RetryAfter)
			}
		}()

		return h(message)
	}
}
