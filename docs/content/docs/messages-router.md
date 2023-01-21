+++
title = "Message Router"
description = "The Magic Glue of Watermill"
date = 2018-12-05T12:48:04+01:00
weight = -850
draft = false
bref = "The Magic Glue of Watermill"
toc = true
+++

[*Publishers and Subscribers*]({{< ref "/docs/pub-sub" >}}) are rather low-level parts of Watermill.
In production use, you'd usually want to use a high-level interface and features like [correlation, metrics, poison queue, retrying, throttling, etc.]({{< ref "/docs/messages-router#middleware" >}}).

You also might not want to send an Ack when processing was successful. Sometimes, you'd like to send a message after processing of another message finishes.

To handle these requirements, there is a component named **Router**.

<img src="/img/watermill-router.svg" alt="Watermill Router" style="width:100%;">

### Configuration

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="type RouterConfig struct {" last_line_contains="RouterConfig) Validate()" padding_after="2" %}}
{{% /render-md %}}

### Handler

At the beginning you need to implement `HandlerFunc`:

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// HandlerFunc is" last_line_contains="type HandlerFunc func" padding_after="1" %}}
{{% /render-md %}}

Next, you have to add a new handler with `Router.AddHandler`:

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// AddHandler" last_line_contains=") {" padding_after="0" %}}
{{% /render-md %}}

See an example usage from [Getting Started]({{< ref "/docs/getting-started#using-messages-router" >}}):
{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/3-router/main.go" first_line_contains="// AddHandler returns a handler" last_line_contains="return h(message)" padding_after="3" %}}
{{% /render-md %}}

### No publisher handler

Not every handler will produce new messages. You can add this kind of handler by using `Router.AddNoPublisherHandler`:

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// AddNoPublisherHandler" last_line_contains=") {" padding_after="0" %}}
{{% /render-md %}}

### Ack

By default, `msg.Ack()` is called when `HanderFunc` doesn't return an error. If an error is returned, `msg.Nack()` will be called.
Because of this, you don't have to call `msg.Ack()` or `msg.Nack()` after a message is processed (you can if you want, of course).

### Producing messages

When returning multiple messages from a handler, be aware that most Publisher implementations don't support [atomic publishing of messages]({{< ref "/docs/pub-sub#publishing-multiple-messages" >}}). It may end up producing only some of messages and sending `msg.Nack()` if the broker or the storage are not available.

If it is an issue, consider publishing just one message with each handler.

### Running the Router

To run the Router, you need to call `Run()`.

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// Run" last_line_contains="func (r *Router) Run(ctx context.Context) (err error) {" padding_after="0" %}}
{{% /render-md %}}

#### Ensuring that the Router is running

It can be useful to know if the router is running. You can use the `Running()` method for this. 

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// Running" last_line_contains="func (r *Router) Running()" padding_after="0" %}}
{{% /render-md %}}

You can also use `IsRunning` function, that returns bool:

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// IsRunning" last_line_contains="func (r *Router) IsRunning()" padding_after="0" %}}
{{% /render-md %}}

#### Closing the Router

To close the Router, you need to call `Close()`.

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// Close gracefully" last_line_contains="func (r *Router) Close()" padding_after="1" %}}
{{% /render-md %}}

`Close()` will close all publishers and subscribers, and wait for all handlers to finish.

`Close()` will wait for a timeout configured in `RouterConfig.CloseTimeout`.
If the timeout is reached, `Close()` will return an error.

### Adding handler after the router has started

You can add a new handler while the router is already running.
To do that, you need to call `AddNoPublisherHandler` or `AddHandler` and call `RunHandlers`.

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// RunHandlers" last_line_contains="func (r *Router) RunHandlers" padding_after="0" %}}
{{% /render-md %}}

### Stopping running handler

It is possible to stop **just one running handler** by calling `Stop()`.

Please keep in mind, that router will be closed when there are no running handlers.

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// Stop" last_line_contains="func (h *Handler) Stop()" padding_after="0" %}}
{{% /render-md %}}

### Execution models

*Subscribers* can consume either one message at a time or multiple messages in parallel.

* **Single stream of messages** is the simplest approach and it means that until a `msg.Ack()` is called, the subscriber
  will not receive any new messages.
* **Multiple message streams** are supported only by some subscribers. By subscribing to multiple topic partitions at once,
  several messages can be consumed in parallel, even previous messages that were not acked (for example, the Kafka subscriber
  works like this). Router handles this model by running concurrent `HandlerFunc`s, one for each partition.
  
See the chosen Pub/Sub documentation for supported execution models.

### Middleware

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// HandlerMiddleware" last_line_contains="type HandlerMiddleware" padding_after="1" %}}
{{% /render-md %}}

A full list of standard middlewares can be found in [Middlewares]({{< ref "/docs/middlewares" >}}).

### Plugin

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router.go" first_line_contains="// RouterPlugin" last_line_contains="type RouterPlugin" padding_after="1" %}}
{{% /render-md %}}

A full list of standard plugins can be found in [message/router/plugin](https://github.com/ThreeDotsLabs/watermill/tree/master/message/router/plugin).

### Context

Each message received by handler holds some useful values in the `context`:

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/router_context.go" first_line_contains="// HandlerNameFromCtx" last_line_contains="func PublishTopicFromCtx" padding_after="2" %}}
{{% /render-md %}}
