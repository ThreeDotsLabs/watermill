+++
title = "Pub/Sub"
description = "Publishers and Subscribers"
date = 2018-12-05T12:47:30+01:00
weight = -900
draft = false
bref = "Publishers and Subscribers"
toc = true
+++

### Publisher

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/pubsub.go" first_line_contains="Publisher interface {" last_line_contains="Close() error" padding_after="1" %}}
{{% /render-md %}}

#### Publishing multiple messages

Most publishers implementations don't support atomic publishing of messages.
This means that if publishing one of the messages fails, the next messages won't be published.

#### Async publish

Publish can be synchronous or asynchronous - it depends on the implementation.

#### `Close()`

`Close` should flush unsent messages if the publisher is asynchronous.
**It is important to not forget to close the subscriber**. Otherwise you may lose some of the messages.

### Subscriber

{{% render-md %}}
{{% load-snippet-partial file="src-link/message/pubsub.go" first_line_contains="Subscriber interface {" last_line_contains="Close() error" padding_after="1" %}}
{{% /render-md %}}

#### Ack/Nack mechanism

It is the *Subscriber's* responsibility to handle an `Ack` and a `Nack` from a message.
A proper implementation should wait for an `Ack` or a `Nack` before consuming the next message.

**Important Subscriber's implementation notice**:
Ack/offset to message's storage/broker **must** be sent after Ack from Watermill's message.
Otherwise there is a chance to lose messages if the process dies before the messages have been processed.

#### `Close()`

`Close` closes all subscriptions with their output channels and flushes offsets, etc. when needed.

### At-least-once delivery

Watermill is built with [at-least-once delivery](http://www.cloudcomputingpatterns.org/at_least_once_delivery/) semantics.
That means when some error occurs when processing a message and an Ack cannot be sent, the message will be redelivered.

You need to keep it in mind and build your application to be [idempotent](http://www.cloudcomputingpatterns.org/idempotent_processor/) or implement a deduplication mechanism.

Unfortunately, it's not possible to create an universal [*middleware*]({{< ref "/docs/messages-router#middleware" >}}) for deduplication, so we encourage you to build your own.

### Universal tests

Every Pub/Sub is similar in most aspects.
To avoid implementing separate tests for every Pub/Sub, we've created a test suite which should be passed by any Pub/Sub
implementation.

These tests can be found in `pubsub/tests/test_pubsub.go`.

### Built-in implementations

To check available Pub/Sub implementations, see [Supported Pub/Subs]({{< ref "/pubsubs" >}}).

### Implementing custom Pub/Sub

See [Implementing custom Pub/Sub]({{< ref "/docs/pub-sub-implementing" >}}) for instructions on how to introduce support for
a new Pub/Sub.

We will also be thankful for submitting [pull requests](https://github.com/ThreeDotsLabs/watermill/pulls) with the new Pub/Sub implementations.

You can also request a new Pub/Sub implementation by submitting a [new issue](https://github.com/ThreeDotsLabs/watermill/issues).

### Keep going!

Now that you already know how a Pub/Sub is working, we recommend learning about the [*Message Router component*]({{< ref "/docs/messages-router" >}}).
