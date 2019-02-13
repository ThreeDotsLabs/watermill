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
{{% load-snippet-partial file="content/src-link/components/metrics/builder.go" first_line_contains="// PrometheusMetricsBuilder" last_line_contains="func (b PrometheusMetricsBuilder)" %}}
{{% /render-md %}}

### Wrapping publishers, subscribers and handlers

If you are using Watermill's [router](/docs/messages-router) (which is recommended in most cases), you can use a single convenience function `AddPrometheusRouterMetrics` to ensure that all the handlers added to this router are wrapped to update the Prometheus registry, together with their publishers and subscribers:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/components/metrics/builder.go" first_line_contains="// AddPrometheusRouterMetrics" last_line_contains="AddMiddleware" padding_after="1" %}}
{{% /render-md %}}

Example use of `AddPrometheusRouterMetrics`:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/_examples/metrics/main.go" first_line_contains="// we leave the namespace" last_line_contains="metricsBuilder.AddPrometheusRouterMetrics" %}}
{{% /render-md %}}

Standalone publishers and subscribers may also be decorated through the use of dedicated methods of `PrometheusMetricBuilder`:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/_examples/metrics/main.go" first_line_contains="subWithMetrics, err := " last_line_contains="pubWithMetrics, err := " padding_after="3" %}}
{{% /render-md %}}

### Exposing the /metrics endpoint

In accordance with how Prometheus works, the service needs to expose a HTTP endpoint for scraping. By convention, it is a GET endpoint, and its path is usually `/metrics`.

To serve this endpoint, there are two convenience functions, one using a previously created Prometheus Registry, while the other also creates a new registry:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/components/metrics/http.go" first_line_contains="// CreateRegistryAndServeHTTP" last_line_contains="func ServeHTTP(" %}}
{{% /render-md %}}

Here is an example of its use in practice:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/_examples/metrics/main.go" first_line_contains="prometheusRegistry, closeMetricsServer :=" last_line_contains="metricsBuilder.AddPrometheusRouterMetrics" %}}
{{% /render-md %}}

### Grafana dashboard

We have prepared a [Grafana dashboard](https://grafana.com/dashboards/9777) to use with the metrics implementation described above. It provides basic information about the throughput, failure rates and publish/handler durations.

For a detailed instruction on how to run the example and use the dashboard on your machine, check [the next section](#example-application).

To find out which metrics are exported to Prometheus, see [Exported metrics](#exported-metrics).

<a target="_blank" href="https://gitlab.com/threedotslabs/threedots.tech/raw/master/static/watermill-io/grafana_dashboard.png"><img src="https://gitlab.com/threedotslabs/threedots.tech/raw/master/static/watermill-io/grafana_dashboard_small.png" /></a>

### Example application

To see how the metrics dashboard works in practice, you can check out the [metrics example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/metrics). 

The docker-compose bundle contains the following services:

#### Golang
 A [Golang](https://hub.docker.com/_/golang) image which runs the [example code](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/metrics/main.go). It consists of a router with a single handler. 
 
 The handler consumes messages from a [Gochannel PubSub](https://github.com/ThreeDotsLabs/watermill/tree/master/message/infrastructure/gochannel), and publishes 0-4 copies of the message with a preconfigured random delay.

Additionally, there is one goroutine which produces messages incoming to the handler with a gochannel publisher, and another goroutine which consumes the messages outgoing from the handler.

The router, the standalone publisher and the standalone subscriber are all decorated with the metrics code and their statistics will appear in the dashboard.

#### Prometheus
[Prometheus](https://hub.docker.com/r/prom/prometheus/), to scrape the metrics which the golang application exposes at `:8081/metrics` by default. It is configured by a [prometheus.yml](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/metrics/prometheus.yml) file, which declares the endpoints that Prometheus will scrape.

#### Grafana
[Grafana](https://hub.docker.com/r/grafana/grafana), to visualize the metrics in a dashboard.

#### Running the example

To run the docker-compose bundle, go to `_examples/metrics` and execute:

```
docker-compose up
```

The golang app will start running, producing messages, passing them through the handler, and consuming the copies.

With default settings, the raw Prometheus metrics should appear at your http://localhost:8081/metrics. 

The Prometheus image should expose a more advanced UI at http://localhost:9090/graph, where you can investigate all the scraped metrics.

However, what is the most useful way to monitor is through the use of Grafana, which you can access at http://localhost:3000. 

#### Adding the Prometheus data source to Grafana

The fresh Grafana image should greet you with a login screen:

![Grafana login screen](https://gitlab.com/threedotslabs/threedots.tech/raw/grafana-dashboard-2/static/watermill-io/grafana_login.png)

Just use the default `admin:admin` credentials. You can skip changing the password, if you wish.

The next thing that we need to do is to add the Prometheus data source. Click on `Add data source`.

In the following screen:

1. Enter a name for the Prometheus data source. Let's name this data source `prometheus`.
1. Choose `Prometheus` from the `Type` dropdown.
1. Enter the `http://localhost:9090` value in the HTTP/URL section.
1. You can leave the remaining settings at default and click `Save & Test`.

![Prometheus data source configuration](https://gitlab.com/threedotslabs/threedots.tech/raw/grafana-dashboard-2/static/watermill-io/prometheus_data_source_config.png)

The Prometheus data source is now ready to use in Grafana.

#### Importing the dashboard

To import the Grafana dashboard, select Dashboard/Manage from the left menu, and then click on `+Import` (or go to http://localhost:3000/dashboard/import).

Enter the dashboard URL https://grafana.com/dashboards/9777 (or just the ID, 9777), and click on Load.

![Importing the dashboard](https://gitlab.com/threedotslabs/threedots.tech/raw/grafana-dashboard-2/static/watermill-io/grafana_import_dashboard.png)

Then select the Prometheus data source created in the previous step. Click on `Import`, and you're done!

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

Additionally, every metric has the `node` label, provided by Prometheus, with value corresponding to the instance that the metric comes from, and `job`, which is the job name specified in the [Prometheus configuration file](https://github.com/ThreeDotsLabs/watermill/blob/master/_examples/metrics/prometheus.yml).

### Customization

If you feel like some metric is missing, you can easily expand this basic implementation. The best way to do so is to use the prometheus registry that is used with [the ServeHTTP method](#exposing-the-metrics-endpoint) and register a metric according to [the documentation](https://godoc.org/github.com/prometheus/client_golang/prometheus) of the Prometheus client.

An elegant way to update these metrics would be through the use of decorators:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/decorator.go" first_line_contains="// MessageTransformSubscriberDecorator" last_line_contains="type messageTransformSubscriberDecorator" %}}
{{% /render-md %}}

and/or [router middlewares](/docs/messages-router/#middleware). 

A more simplistic approach would be to just update the metric that you want in the handler function.

