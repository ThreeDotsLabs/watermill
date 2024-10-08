+++
title = "Redis Stream"
description = "A fast, open source, in-memory, key-value data store"
date = 2023-02-01T22:30:00+08:00
bref = "A fast, open source, in-memory, key-value data store"
+++

### Redis Stream

Redis is the open source, in-memory data store used by millions of developers. Redis stream is a data structure that acts like an append-only log in Redis. We are providing Pub/Sub implementation based on [redis/go-redis](https://github.com/redis/go-redis).

### Installation

```bash
go get github.com/ThreeDotsLabs/watermill-redisstream
```

#### Characteristics

| Feature | Implements | Note |
| ------- | ---------- | ---- |
| ConsumerGroups | yes | |
| ExactlyOnceDelivery | no | |
| GuaranteedOrder | no | |
| Persistent | yes | |
| FanOut | yes | use XREAD to fan out messages when there is no consumer group |

#### Configuration
{{% load-snippet-partial file="src-link/watermill-redisstream/pkg/redisstream/publisher.go" first_line_contains="type PublisherConfig struct" last_line_contains="// Publish publishes message to redis stream" %}}

{{% load-snippet-partial file="src-link/watermill-redisstream/pkg/redisstream/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="func (s *Subscriber) Subscribe" %}}

##### Passing `redis.UniversalClient`

You need to configure and pass your own go-redis client via `Client redis.UniversalClient` in `NewSubscriber` and `NewPublisher`. The client can be either `redis.Client` or `redis.ClusterClient`.

##### Publisher
{{% load-snippet-partial file="src-link/watermill-redisstream/pkg/redisstream/publisher.go" first_line_contains="// NewPublisher" last_line_contains="(*Publisher, error)" padding_after="0" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/redisstream/main.go" first_line_contains="pubClient := redis.NewClient" last_line_contains="panic(err)" padding_after="1" %}}


##### Subscriber
{{% load-snippet-partial file="src-link/watermill-redisstream/pkg/redisstream/subscriber.go" first_line_contains="// NewSubscriber" last_line_contains="(*Subscriber, error)" padding_after="0" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/redisstream/main.go" first_line_contains="subClient := redis.NewClient" last_line_contains="panic(err)" padding_after="1" %}}


#### Publishing

{{% load-snippet-partial file="src-link/watermill-redisstream/pkg/redisstream/publisher.go" first_line_contains="// Publish" last_line_contains="func (p *Publisher) Publish" %}}

#### Subscribing

{{% load-snippet-partial file="src-link/watermill-redisstream/pkg/redisstream/subscriber.go" first_line_contains="func (s *Subscriber) Subscribe" last_line_contains="func (s *Subscriber) Subscribe" %}}

#### Marshaler

Watermill's messages cannot be directly sent to Redis - they need to be marshaled. You can implement your marshaler or use default implementation. The default implementation uses [MessagePack](https://msgpack.org/index.html) for efficient serialization.

{{% load-snippet-partial file="src-link/watermill-redisstream/pkg/redisstream/marshaller.go" first_line_contains="const UUIDHeaderKey" last_line_contains="type DefaultMarshallerUnmarshaller" padding_after="0" %}}
