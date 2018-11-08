package middleware

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/rcrowley/go-metrics"
)

// todo - rewrite (more universal?)
type Metrics struct {
	timer   metrics.Timer
	errs    metrics.Counter
	success metrics.Counter
}

func NewMetrics(timer metrics.Timer, errs metrics.Counter, success metrics.Counter) Metrics {
	return Metrics{timer, errs, success}
}

func (m Metrics) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(event *message.Message) (events []*message.Message, err error) {
		start := time.Now()
		defer func() {
			m.timer.Update(time.Now().Sub(start))
			if err != nil {
				m.errs.Inc(1)
			} else {
				m.success.Inc(1)
			}
		}()

		return h(event)
	}
}

func (m Metrics) ShowStats(interval time.Duration, logger metrics.Logger) {
	go metrics.Log(metrics.DefaultRegistry, interval, logger)
}
