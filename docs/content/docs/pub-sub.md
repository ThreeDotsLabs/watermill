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
{{% load-snippet file="content/src-link/message/publisher.go" %}}
{{% /render-md %}}

#### Publishing multiple messages

Most publishers implementations don't support atomic publishing of messages.
That means, that when publishing one of the messages failed the next messages will be not published.

#### Async publish

Publish can be synchronous or asynchronous - it depends on implementation.

#### `Close()`

Close should flush unsent messages if the publisher is async.
**It's important to not forget to close subscriber**, in the other hand you may lose some of the messages.

### Subscriber

{{% render-md %}}
{{% load-snippet file="content/src-link/message/subscriber.go" %}}
{{% /render-md %}}

#### Ack/Nack mechanism

It is *Subscriber's* responsibility to handle `Ack` and `Nack` from a message.
A proper implementation should wait for `Ack` or `Nack` before consuming the next message.

**Important Subscriber's implementation notice**:
Ack/offset to message's storage/broker **must** be sent after Ack from Watermill's message.
If it wouldn't, it is a possibility to lose messages when the process will die before messages were processed.

#### `Close()`

Close closes all subscriptions with their output channels and flush offsets etc. when needed.

### At-least-once delivery

Watermill is build with [at-least-once delivery](http://www.cloudcomputingpatterns.org/at_least_once_delivery/) semantics.
That means, that when some error with occur when processing message and Ack cannot be sent the message will be redelivered.

You need to keep it in mind and build your application to be [idempotent](http://www.cloudcomputingpatterns.org/idempotent_processor/) or implement deduplication mechanism.

Unfortunately, it's not possible to create universal [*middleware*]({{< ref "/docs/messages-router#middleware" >}}) for deduplication but we encourage you to make your own.

### Universal tests

Every Pub/Sub is similar.
To don't implement separated tests for every Pub/Sub we create test suite which should be passed by any Pub/Sub implementation.

These tests can be found in `message/infrastructure/test_pubsub.go`.

### Built-in implementations

To check available Pub/Subs implementation please check [Pub/Sub's implementations]({{< ref "/docs/pub-sub-implementations" >}})

### Implementing custom Pub/Sub

When any implementation of Pub/Sub. [Implementing custom Pub/Sub]({{< ref "/docs/pub-sub-implementing" >}}).

We will be also thankful for submitting [merge requests](https://github.com/ThreeDotsLabs/watermill/pulls) with new Pub/Subs implementation.

You can also request new Pub/Sub implementation by submitting a [new issue](https://github.com/ThreeDotsLabs/watermill/issues).

### Keep going!

When you already know, how Pub/Sub is working we recommend to check [*Message Router component*]({{< ref "/docs/messages-router" >}}).
