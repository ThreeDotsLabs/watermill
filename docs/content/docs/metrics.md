+++
title = "Metrics"
description = "Monitor Watermill in realtime"
date = 2019-02-12T21:00:00+01:00
weight = -200
draft = false
bref = "Monitor Watermill in realtime"
toc = true
+++

### Metrics

Monitoring of Watermill may be performed by using decorators for publishers/subscribers and middlewares for handlers. 
We provide a default implementation using Prometheus, based on the official [Prometheus client](https://github.com/prometheus/client_golang) for Go.

The `components/metrics` package exports `PrometheusMetricsBuilder`, which provides convenience functions to wrap publishers, subscribers, and handlers so that they update the relevant Prometheus registry:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/components/metrics/builder.go" first_line_contains="// PrometheusMetricsBuilder" last_line_contains="func (b PrometheusMetricsBuilder)" %}}
{{% /render-md %}}

### Wrapping publishers, subscribers, and handlers

If you are using Watermill's [content/messages-router.md](router) (which is recommended in most cases), you can use a single convenience function `AddPrometheusRouterMetrics` to ensure that all the handlers added to this router are wrapped to update the Prometheus registry, together with their publishers and subscribers:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/components/metrics/builder.go" first_line_contains="// AddPrometheusRouterMetrics" last_line_contains="AddMiddleware" padding_after="1" %}}
{{% /render-md %}}

Example use of `AddPrometheusRouterMetrics`:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/_examples/metrics/main.go" first_line_contains="prometheusRegistry :=" last_line_contains="AddPrometheusRouterMetrics(" %}}
{{% /render-md %}}

Standalone publishers and subscribers may also be decorated through the use of dedicated methods of `PrometheusMetricBuilder`:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/_examples/metrics/main.go" first_line_contains="subWithMetrics, err := " last_line_contains="pubWithMetrics, err := " padding_after="3" %}}
{{% /render-md %}}

### Exposing the /metrics endpoint

todo...
