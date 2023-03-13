+++
title = "Metrics"
description = "Monitor Watermill in realtime"
date = 2019-02-12T21:00:00+01:00
weight = -200
draft = false
bref = "Monitor Watermill in realtime using Prometheus"
toc = true
+++

### Metrics

Monitoring of Watermill may be performed by using decorators for publishers/subscribers and middlewares for handlers. 
We provide a default implementation using Prometheus, based on the official [Prometheus client](https://github.com/prometheus/client_golang) for Go.

The `components/metrics` package exports `PrometheusMetricsBuilder`, which provides convenience functions to wrap publishers, subscribers and handlers so that they update the relevant Prometheus registry:

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/metrics/builder.go" first_line_contains="// PrometheusMetricsBuilder" last_line_contains="func (b PrometheusMetricsBuilder)" %}}
{{% /render-md %}}

### Wrapping publishers, subscribers and handlers

If you are using Watermill's [router](/docs/messages-router) (which is recommended in most cases), you can use a single convenience function `AddPrometheusRouterMetrics` to ensure that all the handlers added to this router are wrapped to update the Prometheus registry, together with their publishers and subscribers:

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/metrics/builder.go" first_line_contains="// AddPrometheusRouterMetrics" last_line_contains="AddMiddleware" padding_after="1" %}}
{{% /render-md %}}

Example use of `AddPrometheusRouterMetrics`:

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/4-metrics/main.go" first_line_contains="// we leave the namespace" last_line_contains="metricsBuilder.AddPrometheusRouterMetrics" %}}
{{% /render-md %}}

In the snippet above, we have left the `namespace` and `subsystem` arguments empty. The Prometheus client library [uses these](https://godoc.org/github.com/prometheus/client_golang/prometheus#BuildFQName) to prefix the metric names. You may want to use namespace or subsystem, but be aware that this will impact the metric names and you will have to adjust the Grafana dashboard accordingly.

Standalone publishers and subscribers may also be decorated through the use of dedicated methods of `PrometheusMetricBuilder`:

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/4-metrics/main.go" first_line_contains="subWithMetrics, err := " last_line_contains="pubWithMetrics, err := " padding_after="3" %}}
{{% /render-md %}}

### Exposing the /metrics endpoint

In accordance with how Prometheus works, the service needs to expose a HTTP endpoint for scraping. By convention, it is a GET endpoint, and its path is usually `/metrics`.

To serve this endpoint, there are two convenience functions, one using a previously created Prometheus Registry, while the other also creates a new registry:

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/metrics/http.go" first_line_contains="// CreateRegistryAndServeHTTP" last_line_contains="func ServeHTTP(" %}}
{{% /render-md %}}

Here is an example of its use in practice:

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/4-metrics/main.go" first_line_contains="prometheusRegistry, closeMetricsServer :=" last_line_contains="metricsBuilder.AddPrometheusRouterMetrics" %}}
{{% /render-md %}}

### Example application

To see how the metrics dashboard works in practice, you can check out the [metrics example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/basic/4-metrics). 

Follow the instructions in the example's [README](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/basic/4-metrics/README.md) to make it run and add the Prometheus data source to Grafana.

### Grafana dashboard

We have prepared a [Grafana dashboard](https://grafana.com/grafana/dashboards/9777-watermill/) to use with the metrics implementation described above. It provides basic information about the throughput, failure rates and publish/handler durations.

If you want to check out the dashboard on your machine, you can use the [Example application](#example-application).

To find out more about the metrics that are exported to Prometheus, see [Exported metrics](#exported-metrics).

<a target="_blank" href="https://threedots.tech/watermill-io/grafana_dashboard.png"><img src="https://threedots.tech/watermill-io/grafana_dashboard_small.png" /></a>

#### Importing the dashboard

To import the Grafana dashboard, select Dashboard/Manage from the left menu, and then click on `+Import`.

Enter the dashboard URL https://grafana.com/dashboards/9777 (or just the ID, 9777), and click on Load.

![Importing the dashboard](https://threedots.tech/watermill-io/grafana_import_dashboard.png)

Then select your the Prometheus data source that scrapes the `/metrics` endpoint. Click on `Import`, and you're done!

### Exported metrics

Listed below are all the metrics that are registered on the Prometheus Registry by `PrometheusMetricsBuilder`.
 
For more information on Prometheus metric types, please refer to [Prometheus docs](https://prometheus.io/docs/concepts/metric_types).
 
<table>
  <tr>
    <th>Object</th>
    <th>Metric</th>
    <th>Description</th>
    <th>Labels/Values</th>
  </tr>
  <tr>
    <td rowspan="3">Subscriber</td>
    <td rowspan="3"><code>subscriber_messages_received_total</code></td>
    <td rowspan="3">A Prometheus Counter.<br>Counts the number of messages obtained by the subscriber.</td>
    <td><code>acked</code> is either "acked" or "nacked".</td>
  </tr>
  <tr>
    <td><code>handler_name</code> is set if the subscriber operates within a handler; "&lt;no handler&gt;" otherwise.</td>
  </tr>
  <tr>
    <td><code>subscriber_name</code> identifies the subscriber. If it implements <code>fmt.Stringer</code>, it is the result of `String()`, <code>package.structName</code> otherwise.</td>
  </tr>
  <tr>
    <td rowspan="2">Handler</td>
    <td rowspan="2"><code>handler_execution_time_seconds</code></td>
    <td rowspan="2">A Prometheus Histogram. <br>Registers the execution time of the handler function wrapped by the middleware.</td>
    <td><code>handler_name</code> is the name of the handler.</td>
  </tr>
  <tr>
    <td><code>success</code> is either "true" or "false", depending on whether the wrapped handler function returned an error or not.</td>
  </tr>
  <tr>
    <td rowspan="3">Publisher</td>
    <td rowspan="3"><code>publish_time_seconds</code></td>
    <td rowspan="3">A Prometheus Histogram.<br>Registers the time of execution of the Publish function of the decorated publisher.</td>
    <td><code>success</code> is either "true" or "false", depending on whether the decorated publisher returned an error or not.</td>
  </tr>
  <tr>
    <td><code>handler_name</code> is set if the publisher operates within a handler; "&lt;no handler&gt;" otherwise.</td>
  </tr>
  <tr>
    <td><code>publisher_name</code> identifies the publisher. If it implements <code>fmt.Stringer</code>, it is the result of `String()`, <code>package.structName</code> otherwise.</td>
  </tr>
</table>

Additionally, every metric has the `node` label, provided by Prometheus, with value corresponding to the instance that the metric comes from, and `job`, which is the job name specified in the [Prometheus configuration file](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/basic/4-metrics/prometheus.yml).

**NOTE**: As described [above](#wrapping-publishers-subscribers-and-handlers), using non-empty `namespace` or `subsystem` will result in prefixed metric names. You might need to adjust for it, for example in the definitions of panels in the Grafana dashboard.

### Customization

If you feel like some metric is missing, you can easily expand this basic implementation. The best way to do so is to use the prometheus registry that is used with the [ServeHTTP method](#exposing-the-metrics-endpoint) and register a metric according to [the documentation](https://godoc.org/github.com/prometheus/client_golang/prometheus) of the Prometheus client.

An elegant way to update these metrics would be through the use of decorators:

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/decorator.go" first_line_contains="// MessageTransformSubscriberDecorator" last_line_contains="type messageTransformSubscriberDecorator" %}}
{{% /render-md %}}

and/or [router middlewares](/docs/messages-router/#middleware). 

A more simplistic approach would be to just update the metric that you want in the handler function.

