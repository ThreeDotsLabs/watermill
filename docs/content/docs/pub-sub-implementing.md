+++
title = "Implementing custom Pub/Sub"
description = "Bring Your Own Pub/Sub"
date = 2018-12-05T12:48:34+01:00
weight = -300
draft = false
bref = "Bring Your Own Pub/Sub"
toc = true
+++

### Pub/Sub interface

In simple words - to implement Pub/Sub you need to implement `message.PubSub` interface.

{{% render-md %}}
{{% load-snippet file="content/src-link/message/publisher.go" %}}
{{% load-snippet file="content/src-link/message/subscriber.go" %}}
{{% load-snippet-partial file="content/src-link/message/pubsub.go" first_line_contains="type PubSub interface {" last_line_contains="Close() error" padding_after="1" %}}
{{% /render-md %}}

### TODO list

But they are some things about which you cannot forget:

1. Good logging
2. Replaceable and configureable messages marshaler
3. `Close()` implementation for publisher and subscriber which is:
    - idempotent
    - working event when publisher or subscriber is blocked (for example: when waiting for Ack)
    - working when subscriber output channel is blocked (because nothing is listening for it)
4. `Ack()` **and** `Nack()` support for consumed messages
5. Redelivery on `Nack()` on consumed message
6. Use [Universal Pub/Sub tests]({{< ref "/docs/pub-sub#universal-tests" >}})
7. Performance optimizations
8. godoc's, [Markdown docs]({{< ref "/docs/pub-sub-implementations" >}}) and [examples Getting Started](/docs/getting-started)

We will be also thankful for submitting [merge requests](https://github.com/ThreeDotsLabs/watermill/pulls) with new Pub/Subs implementation.

If anything is not clear feel free to use any of our [support channels]({{< ref "/support" >}}), we will we'll be glad to help.
