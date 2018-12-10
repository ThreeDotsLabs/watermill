+++
title = "Message Router"
description = "Magic glue of Watermill"
date = 2018-12-05T12:48:04+01:00
weight = -850
draft = false
bref = ""
toc = true
+++

[*Publishers and subscribers*]({{< ref "docs/pub-sub" >}}) are rather low-level parts of Watermill.
In production use we want usually use something which is higher level and provides some features like [correlation, metrics, poison queue, retrying, throttling etc]({{< ref "/docs/messages-router-middleware" >}}).

We also don't want to manually send Ack when processing was successful. Sometimes, we also want send a message after processing another.

To handle these requirements we created component named [*Router*]({{< ref "docs/messages-router" >}}).

[todo - schema]

### Configuration

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="type RouterConfig struct {" last_line_contains="RouterConfig) Validate()" padding_after="2" %}}
{{% /render-md %}}

### Handler

At the beginning we need to implement HandlerFunc:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// HandlerFunc is" last_line_contains="type HandlerFunc func" padding_after="1" %}}
{{% /render-md %}}

Next we need to add a new handler with `Router.AddHandler`:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// AddHandler" last_line_contains=") error" padding_after="0" %}}
{{% /render-md %}}

And example usage from [Getting Started]({{< ref "docs/getting-started#using-messages-router" >}}):
{{% render-md %}}
{{% load-snippet-partial file="content/docs/getting-started/router/main.go" first_line_contains="if err := router.AddHandler(" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

#### No publisher handler

Not every handler needs to publish messages.
You can add this kind of handler by using `Router.AddNoPublisherHandler`:

{{% render-md %}}
{{% load-snippet-partial file="content/src-link/message/router.go" first_line_contains="// AddNoPublisherHandler" last_line_contains=") error" padding_after="0" %}}
{{% /render-md %}}

#### Ack

You don't need to manually call `msg.Ack()` or `msg.Nack()` after message is processed (but you can, of course).
`msg.Ack()` is called when `HanderFunc` doesn't return error. If error was returned, `msg.Nack()` will be called.

#### Producing messages

When returning multiple messages in router,
you should be aware that most of Publisher's implementations doesn't support [atomically publishing of the messages]({{< ref "docs/pub-sub#publishing-multiple-messages" >}}).

It may lead to producing only part of the messages and sending `msg.Nack()` when broker or storage is not available.

When it is a problem, you should consider publishing maximum one message with one handler.

#### Execution model

Some *Consumers* may support only single stream of messages - that means that until `msg.Ack()` is sent you will not receive more messages.

But some *Consumers* can for example subscribe to multiple partitions in parallel and multiple messages will be sent even previous was not Acked (Kafka Consumer for example).
Router can handle this case and spawn multiple HandlerFunc in parallel.

### Middleware

[todo - list link]

### Plugin

[todo - list link]