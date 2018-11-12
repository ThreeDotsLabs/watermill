package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rcrowley/go-metrics"
)

func newMetricsMiddleware() middleware.Metrics {
	t := metrics.NewTimer()
	metrics.Register("handler.time", t)

	errs := metrics.NewCounter()
	metrics.Register("handler.errors", errs)

	success := metrics.NewCounter()
	metrics.Register("handler.success", success)

	pClient := prometheusmetrics.NewPrometheusProvider(
		metrics.DefaultRegistry,
		"test",
		"subsys",
		prometheus.DefaultRegisterer,
		1*time.Second,
	)
	go pClient.UpdatePrometheusMetrics()
	http.Handle("/metrics", promhttp.Handler())

	go http.ListenAndServe(":9000", nil)
	metricsMiddleware := middleware.NewMetrics(t, errs, success)
	metricsMiddleware.ShowStats(time.Second*5, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	return metricsMiddleware
}
