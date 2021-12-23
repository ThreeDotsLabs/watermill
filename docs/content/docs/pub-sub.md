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

### Supported Features

Each Pub/Sub's documentation starts with a table of supported features, described below.

#### ConsumerGroups

When specified as “yes” this parameter means that the Pub/Sub supports consumer groups.

A consumer group is a set of subscribers that consume messages as a single entity.

A message is delivered to a single consumer group.
Watermill picks one of the subscribers in this group and delivers the message to it.
Other members of the same group won’t see this message (unless it’s nacked and re-delivered later).

Usually, consumer groups are specified with a string key.
This can be anything as long as it’s a unique identifier.
If it’s an empty string, the subscriber is not a member of any group and will receive all messages.

Note: Consumer Groups come from Kafka’s nomenclature, but other Pub/Subs have very similar mechanisms that are called differently.
For example, in Google Cloud Pub/Sub you can have multiple subscribers subscribed to the same subscription name.

#### ExactlyOnceDelivery

When specified as “yes” this parameter means that the Pub/Sub supports exactly once delivery of messages.

Exactly once delivery means the message will be delivered once, and only once, if it’s acknowledged by the subscriber.

The more common scenario is “at-least once delivery”.
This means that the message will be delivered, but something can happen that makes it re-delivered another time, even after being acknowledged.

The reason for this is that the subscriber correctly does some action
(like saving an aggregate in the database and committing the transaction), but then acknowledging the message fails for some reason (e.g., broken network).
It’s too late to rollback the database transaction, and the message will be delivered again.

At-least once delivery is the default for many Pub/Subs, and subscribers should be aware most of the time that the message handlers should be idempotent.
They should be ready to handle the same message twice with no side-effects in the system.
Watermill provides a Duplicator middleware that duplicates the incoming messages that you can plug in on the development environment to see if your handlers work correctly.

Exactly-once delivery can be achieved only in a very specific case: when the handler saves data in the same database that’s used as a Pub/Sub.
The most common way to do it is to use an SQL database as a Pub/Sub. There’s an example from Robert that shows this.

GoChannel is also considered exactly-once delivery, since there’s no network that can break the acknowledgement.

Another example is Kafka if all your handlers do is publish messages on Kafka and you use the Transaction feature.
At the time of implementing watermill-kafka no Go client supported these, though.

#### GuaranteedOrder

When specified as “yes” this parameter means that the Pub/Sub supports guaranteed order of messages.

A guaranteed order means the messages are delivered to subscribers in exactly the same order they have been published on the Pub/Sub.

To achieve this, a message is delivered to the subscriber only after the previous message has been correctly processed and acknowledged.
Effectively, this means you can process one message at a time.

Some Pub/Subs support this way of working only for a single subscriber.

It’s a rare feature overall, as it means you can’t scale your message handlers horizontally if they use a single partition.
In most cases, you would instead make your handlers ready to handle messages out of order, e.g.,
rely on a version or a timestamp field in the messages’s payload or metadata instead of the order they are delivered.

#### Persistent

When specified as “yes” this parameter means that the Pub/Sub supports persistence of messages.

A Pub/Sub is persistent if another Pub/Sub instance created with the same config is able to send and receive the same messages.
In other words, the messages are not stored in-memory but in some external storage.

In practice, GoChannel is the only Pub/Sub that doesn’t support this, since all messages are stored in memory.

### Implementing custom Pub/Sub

See [Implementing custom Pub/Sub]({{< ref "/docs/pub-sub-implementing" >}}) for instructions on how to introduce support for
a new Pub/Sub.

We will also be thankful for submitting [pull requests](https://github.com/ThreeDotsLabs/watermill/pulls) with the new Pub/Sub implementations.

You can also request a new Pub/Sub implementation by submitting a [new issue](https://github.com/ThreeDotsLabs/watermill/issues).

### Keep going!

Now that you already know how a Pub/Sub is working, we recommend learning about the [*Message Router component*]({{< ref "/docs/messages-router" >}}).
