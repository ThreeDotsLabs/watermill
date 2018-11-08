package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
	"github.com/throttled/throttled"
	"github.com/throttled/throttled/store/memstore"
)

type Throttle struct {
	rateLimiter throttled.RateLimiter
	logger      watermill.LoggerAdapter
}

func NewThrottlePerSecond(perSecond int, logger watermill.LoggerAdapter) (Throttle, error) {
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
	return func(message *message.Message) ([]*message.Message, error) {
		defer func() {
			limited, context, err := t.rateLimiter.RateLimit("", 1)
			if err != nil {
				t.logger.Info("Throttle error", watermill.LogFields{"err": err.Error()})
			}
			if limited {
				t.logger.Info("Throttling", watermill.LogFields{"wait_duration": context.RetryAfter.String()})
				time.Sleep(context.RetryAfter)
			}
		}()

		return h(message)
	}
}
