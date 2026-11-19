+++
title = "Implementing a new Pub/Sub"
description = "Bring Your Own Pub/Sub"
date = 2018-12-05T12:48:34+01:00
weight = 200
draft = false
bref = "Bring Your Own Pub/Sub"
+++

## The Pub/Sub interface

To add support for a custom Pub/Sub, you have to implement both `message.Publisher` and `message.Subscriber` interfaces.

{{% load-snippet-partial file="src-link/message/pubsub.go" first_line_contains="type Publisher interface" last_line_contains="type SubscribeInitializer" padding_after="0" %}}

## Testing

Watermill provides [a set of test scenarios](https://github.com/ThreeDotsLabs/watermill/blob/master/pubsub/tests/test_pubsub.go)
that any Pub/Sub implementation can use. Each test suite needs to declare what features it supports and how to construct a new Pub/Sub.
These scenarios check both basic usage and more uncommon use cases. Stress tests are also included.

## TODO list

Here are a few things you shouldn't forget about:

1. Logging (good messages and proper levels).
2. Replaceable and configurable message marshaler.
3. `Close()` implementation for the publisher and subscriber that is:
    - idempotent
    - working correctly even when the publisher or the subscriber is blocked (for example, waiting for an Ack).
    - working correctly when the subscriber output channel is blocked (because nothing is listening on it).
4. `Ack()` **and** `Nack()` support for consumed messages.
5. Redelivery on `Nack()` for a consumed message.
6. Use [Universal Pub/Sub tests]({{< ref "/docs/pub-sub#universal-tests" >}}). For debugging tips, you should check [tests troubleshooting guide](/docs/troubleshooting/#debugging-pubsub-tests).
7. Performance optimizations.
8. GoDocs, [Markdown docs]({{< ref "/pubsubs" >}}) and [Getting Started examples](/learn/getting-started).

We will also be thankful for submitting a [pull request](https://github.com/ThreeDotsLabs/watermill/pulls) with the new Pub/Sub implementation.

If anything is not clear, feel free to use any of our [support channels]({{< ref "/support" >}}) to reach us, we will be glad to help.

## Inspector support

To show what's behind a topic in the [inspector](/advanced/inspector/), implement `topology.Describer`
on the publisher and the subscriber:

{{% load-snippet-partial file="src-link/topology/topology.go" first_line_contains="// Describer can be implemented" last_line_contains="type Resource struct" padding_after="3" %}}

A publisher and a subscriber connected to the same system must return the same `PubSub.ID`.
Many services can be connected to the same system (like a Kafka cluster), so use an ID that every service computes the same way.
If the system exists only in one process (like GoChannel), set `Local: true` instead.
Addresses don't work for this: services often reach the same broker under different addresses.

`DescribeTopic` is called for every published message, so keep it cheap.

```go
func describeTopic(clusterID string, topic string) topology.Description {
    return topology.Description{
        PubSub: topology.PubSub{ID: clusterID, Type: "kafka", Name: "Kafka"},
        Resources: []topology.Resource{
            {Kind: "topic", Name: topic},
        },
    }
}

func (p *Publisher) DescribeTopic(topic string) topology.Description {
    return describeTopic(p.clusterID, topic)
}

func (s *Subscriber) DescribeTopic(topic string) topology.Description {
    d := describeTopic(s.clusterID, topic)
    // resources that belong only to this subscriber
    d.SubscriptionResources = []topology.Resource{
        {Kind: "consumer group", Name: s.config.ConsumerGroup},
    }
    return d
}
```

The publisher should also report each message it publishes, so messages published outside a router are visible:

```go
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
    if registry := topology.Current(); registry != nil {
        for _, msg := range messages {
            registry.ReportPublished(p, topic, msg.Context(), msg.UUID, msg.Metadata, msg.Payload)
        }
    }

    // ...
}
```

`topology.Current()` returns `nil` when the inspector is not enabled.