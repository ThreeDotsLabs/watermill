+++
title = "Implementing custom Pub/Sub"
description = "Bring Your Own Pub/Sub"
date = 2018-12-05T12:48:34+01:00
weight = -300
draft = false
bref = "Bring Your Own Pub/Sub"
toc = true
+++

### The Pub/Sub interface

To add support for a custom Pub/Sub, you have to implement both `message.Publisher` and `message.Subscriber` interfaces.

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/pubsub.go" first_line_contains="type Publisher interface" last_line_contains="type SubscribeInitializer" padding_after="0" %}}
{{% /render-md %}}

### TODO list

Here are a few things you shouldn't forget about:

1. Logging (good messages and proper levels).
2. Replaceable and configurable messages marshaller.
3. `Close()` implementation for the publisher and subscriber that is:
    - idempotent
    - working correctly even when the publisher or the subscriber is blocked (for example, waiting for an Ack).
    - working correctly when the subscriber output channel is blocked (because nothing is listening on it).
4. `Ack()` **and** `Nack()` support for consumed messages.
5. Redelivery on `Nack()` for a consumed message.
6. Use [Universal Pub/Sub tests]({{< ref "/docs/pub-sub#universal-tests" >}}). For debugging tips, you should check [tests troubleshooting guide](/docs/troubleshooting/#debugging-pubsub-tests).
7. Performance optimizations.
8. GoDocs, [Markdown docs]({{< ref "/pubsubs" >}}) and [Getting Started examples](/docs/getting-started).

We will also be thankful for submitting a [pull requests](https://github.com/ThreeDotsLabs/watermill/pulls) with the new Pub/Sub implementation.

If anything is not clear, feel free to use any of our [support channels]({{< ref "/support" >}}) to reach us, we will be glad to help.
