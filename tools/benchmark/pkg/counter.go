package pkg

import (
	"sync/atomic"
	"time"
)

type Counter struct {
	count     uint64
	startTime time.Time
}

func NewCounter() *Counter {
	return &Counter{
		count:     0,
		startTime: time.Now(),
	}
}

func (c *Counter) Add(n uint64) {
	atomic.AddUint64(&c.count, n)
}

func (c *Counter) Count() uint64 {
	return c.count
}

func (c *Counter) MeanPerSecond() float64 {
	return float64(c.count) / time.Since(c.startTime).Seconds()
}
