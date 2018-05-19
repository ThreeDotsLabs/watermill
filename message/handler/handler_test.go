package handler

import (
	"testing"
	"time"
	"fmt"
	"github.com/roblaszczak/gooddd/message/handler/handler"
)

func TestFunctional(t *testing.T) {
	router := handler.NewRouter("example", listenerFactory)

	metricsMiddleware := middleware.NewMetrics(t, errs, success)
	metricsMiddleware.ShowStats(time.Second*5, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	retryMiddleware := middleware.NewRetry()
	retryMiddleware.OnRetryHook = func(retryNum int, delay time.Duration) {
		//fmt.Println("retrying, num:", retryNum, "delay:", delay)
	}
	retryMiddleware.MaxRetries = 1
	retryMiddleware.WaitTime = time.Millisecond * 10

	router.AddMiddleware(
		metricsMiddleware.Middleware,
		middleware.PoisonQueueHook(func(message handler.Message) {
			fmt.Println("unable to process", message)
		}),
		retryMiddleware.Middleware,
		middleware.Recoverer,
		middleware.RandomFail(0.002),
		middleware.RandomPanic(0.002),
	)

	router.AddPlugin(plugin.SignalsHandler)

	router.Subscribe(
		"posts_counter",
		"test_topic",
		counter.Count,
	)
	router.Subscribe(
		"feed_generator",
		"test_topic",
		feedGenerator.UpdateFeed,
	)
}
