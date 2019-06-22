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
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="type RouterConfig struct {" last_line_contains="RouterConfig) Validate()" padding_after="2" %}}
{{% /render-md %}}

### Handler

At the beginning you need to implement `HandlerFunc`:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// HandlerFunc is" last_line_contains="type HandlerFunc func" padding_after="1" %}}
{{% /render-md %}}

Next, you have to add a new handler with `Router.AddHandler`:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// AddHandler" last_line_contains=") {" padding_after="0" %}}
{{% /render-md %}}

See an example usage from [Getting Started]({{< ref "/docs/getting-started#using-messages-router" >}}):
{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="router.AddHandler(" last_line_contains="structHandler{}.Handler," padding_after="1" %}}
{{% /render-md %}}

### No publisher handler

Not every handler needs to publish messages.
You can add this kind of handler by using `Router.AddNoPublisherHandler`:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// AddNoPublisherHandler" last_line_contains=") {" padding_after="0" %}}
{{% /render-md %}}

### Ack

You don't have to call `msg.Ack()` or `msg.Nack()` after a message is processed (you can if you want, of course).
`msg.Ack()` is called when `HanderFunc` doesn't return an error. If an error is returned, `msg.Nack()` will be called.

### Producing messages

When returning multiple messages in the router,
you should be aware that most of Publisher's implementations don't support [atomic publishing of the messages]({{< ref "/docs/pub-sub#publishing-multiple-messages" >}}).

It may lead to producing only some of the messages and sending `msg.Nack()` when the broker or the storage are not available.

If it is an issue, you should consider publishing a maximum of one message with one handler.

### Running the Router

To run the Router, you need to call `Run()`.

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// Run" last_line_contains="func (r *Router) Run(ctx context.Context) (err error) {" padding_after="0" %}}
{{% /render-md %}}

#### Ensuring that the Router is running

You might sometimes want to do something after the router starts. You can use the `Running()` method for this.

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// Running" last_line_contains="func (r *Router) Running()" padding_after="0" %}}
{{% /render-md %}}

### Execution model

Some *Consumers* may support only a single stream of messages - this means that until a `msg.Ack()` is sent, you will not receive any more messages.

However, some *Consumers* support multiple streams. For example, by subscribing to multiple partitions in parallel,
multiple messages will be sent, even previous that were not Acked (e.g. the Kafka Consumer works like this).
The router can handle this case and spawn multiple `HandlerFunc` in parallel.

### Middleware

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// HandlerMiddleware" last_line_contains="type HandlerMiddleware" padding_after="1" %}}
{{% /render-md %}}

A full list of the standard middlewares can be found in [Middlewares]({{< ref "/docs/middlewares" >}}).

### Plugin

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// RouterPlugin" last_line_contains="type RouterPlugin" padding_after="1" %}}
{{% /render-md %}}

A full list of the standard plugins can be found in [message/router/plugin](https://github.com/ThreeDotsLabs/watermill/tree/master/message/router/plugin).
