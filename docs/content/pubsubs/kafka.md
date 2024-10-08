+++
title = "Kafka"
description = "A distributed streaming platform from Apache"
date = 2019-07-06T22:30:00+02:00
bref = "A distributed streaming platform from Apache"
weight = 80
+++

Apache Kafka is one of the most popular Pub/Subs. We are providing Pub/Sub implementation based on [Shopify's Sarama](https://github.com/Shopify/sarama).

### Installation

```bash
go get github.com/ThreeDotsLabs/watermill-kafka/v3
```

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | yes | |
| ExactlyOnceDelivery | no | in theory can be achieved with [Transactions](https://www.confluent.io/blog/transactions-apache-kafka/), currently no support for any Golang client  |
| GuaranteedOrder | yes | require [partition key usage](#partitioning)  |
| Persistent | yes| |

#### Configuration

{{% load-snippet-partial file="src-link/watermill-kafka/pkg/kafka/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="// Subscribe" %}}

##### Passing custom `Sarama` config

You can pass [custom config](https://github.com/Shopify/sarama/blob/master/config.go#L20) parameters via `overwriteSaramaConfig *sarama.Config` in `NewSubscriber` and `NewPublisher`.
When `nil` is passed, default config is used (`DefaultSaramaSubscriberConfig`).

{{% load-snippet-partial file="src-link/watermill-kafka/pkg/kafka/subscriber.go" first_line_contains="// DefaultSaramaSubscriberConfig" last_line_contains="return config" padding_after="1" %}}

#### Connecting

##### Publisher
{{% load-snippet-partial file="src-link/watermill-kafka/pkg/kafka/publisher.go" first_line_contains="// NewPublisher" last_line_contains="(*Publisher, error)" padding_after="0" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/kafka/main.go" first_line_contains="saramaSubscriberConfig :=" last_line_contains="panic(err)" padding_after="1" %}}


##### Subscriber
{{% load-snippet-partial file="src-link/watermill-kafka/pkg/kafka/subscriber.go" first_line_contains="// NewSubscriber" last_line_contains="(*Subscriber, error)" padding_after="0" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/kafka/main.go" first_line_contains="publisher, err := kafka.NewPublisher" last_line_contains="panic(err)" padding_after="1" %}}

#### Publishing

{{% load-snippet-partial file="src-link/watermill-kafka/pkg/kafka/publisher.go" first_line_contains="// Publish" last_line_contains="func (p *Publisher) Publish" %}}

#### Subscribing

{{% load-snippet-partial file="src-link/watermill-kafka/pkg/kafka/subscriber.go" first_line_contains="// Subscribe" last_line_contains="func (s *Subscriber) Subscribe" %}}

#### Marshaler

Watermill's messages cannot be directly sent to Kafka - they need to be marshaled. You can implement your marshaler or use default implementation.

{{% load-snippet-partial file="src-link/watermill-kafka/pkg/kafka/marshaler.go" first_line_contains="// Marshaler" last_line_contains="func (DefaultMarshaler)" padding_after="0" %}}

#### Partitioning

Our Publisher has support for the partitioning mechanism.

It can be done with special Marshaler implementation:

{{% load-snippet-partial file="src-link/watermill-kafka/pkg/kafka/marshaler.go" first_line_contains="type kafkaJsonWithPartitioning" last_line_contains="func (j kafkaJsonWithPartitioning) Marshal" padding_after="0" %}}

When using, you need to pass your function to generate partition key.
It's a good idea to pass this partition key with metadata to not unmarshal entire message.

```go
marshaler := kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
    return msg.Metadata.Get("partition"), nil
})
```
