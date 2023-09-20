# Watermill
<img align="right" width="200" src="https://threedots.tech/watermill-io/watermill-logo.png">

[![CI Status](https://github.com/ThreeDotsLabs/watermill/actions/workflows/master.yml/badge.svg)](https://github.com/ThreeDotsLabs/watermill/actions/workflows/master.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ThreeDotsLabs/watermill.svg)](https://pkg.go.dev/github.com/ThreeDotsLabs/watermill)
[![Go Report Card](https://goreportcard.com/badge/github.com/ThreeDotsLabs/watermill)](https://goreportcard.com/report/github.com/ThreeDotsLabs/watermill)
[![codecov](https://codecov.io/gh/ThreeDotsLabs/watermill/branch/master/graph/badge.svg)](https://codecov.io/gh/ThreeDotsLabs/watermill)

Watermill is a Go library for working efficiently with message streams. It is intended
for building event driven applications, enabling event sourcing, RPC over messages,
sagas and basically whatever else comes to your mind. You can use conventional pub/sub
implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog if that fits your use case.

## Goals

* **Easy** to understand.
* **Universal** - event-driven architecture, messaging, stream processing, CQRS - use it for whatever you need.
* **Fast** (see [Benchmarks](#benchmarks)).
* **Flexible** with middlewares, plugins and Pub/Sub configurations.
* **Resilient** - using proven technologies and passing stress tests (see [Stability](#stability)).

## Getting Started

Pick what you like the best or see in order:

1. Follow the [Getting Started guide](https://watermill.io/docs/getting-started/).
2. See examples below.
3. Read the full documentation: https://watermill.io/

## Our online hands-on training

<a href="https://threedots.tech/event-driven/?utm_source=watermill-readme"><img align="center" width="400" src="https://threedots.tech/event-driven-banner.png"></a>

## Examples

* Basic
    * [Your first app](_examples/basic/1-your-first-app) - **start here!**
    * [Realtime feed](_examples/basic/2-realtime-feed)
    * [Router](_examples/basic/3-router)
    * [Metrics](_examples/basic/4-metrics)
    * [CQRS with protobuf](_examples/basic/5-cqrs-protobuf)
* [Pub/Subs usage](_examples/pubsubs)
    * These examples are part of the [Getting started guide](https://watermill.io/docs/getting-started/) and show usage of a single Pub/Sub at a time.
* Real-world examples
    * [Exactly-once delivery counter](_examples/real-world-examples/exactly-once-delivery-counter)
    * [Receiving webhooks](_examples/real-world-examples/receiving-webhooks)
    * [Sending webhooks](_examples/real-world-examples/sending-webhooks)
    * [Synchronizing Databases](_examples/real-world-examples/synchronizing-databases)
    * [Persistent Event Log](_examples/real-world-examples/persistent-event-log)
    * [Transactional Events](_examples/real-world-examples/transactional-events)
    * [Real-time HTTP updates with Server-Sent Events](_examples/real-world-examples/server-sent-events)
* Complete projects
    * [NATS example with live code reloading](https://github.com/ThreeDotsLabs/nats-example)
    * [RabbitMQ, webhooks and Kafka integration](https://github.com/ThreeDotsLabs/event-driven-example)

## Background

Building distributed and scalable services is rarely as easy as some may suggest. There is a
lot of hidden knowledge that comes with writing such systems. Just like you don't need to know the
whole TCP stack to create a HTTP REST server, you shouldn't need to study all of this knowledge to
start with building message-driven applications.

Watermill's goal is to make communication with messages as easy to use as HTTP routers. It provides
the tools needed to begin working with event-driven architecture and allows you to learn the details
on the go.

At the heart of Watermill there is one simple interface:
```go
func(*Message) ([]*Message, error)
```

Your handler receives a message and decides whether to publish new message(s) or return
an error. What happens next is up to the middlewares you've chosen.

You can find more about our motivations in our [*Introducing Watermill* blog post](https://threedots.tech/post/introducing-watermill/).

## Pub/Subs

All publishers and subscribers have to implement an interface:

```go
type Publisher interface {
	Publish(topic string, messages ...*Message) error
	Close() error
}

type Subscriber interface {
	Subscribe(ctx context.Context, topic string) (<-chan *Message, error)
	Close() error
}
```

Supported Pub/Subs:

- AMQP Pub/Sub [(`github.com/ThreeDotsLabs/watermill-amqp/v2`)](https://github.com/ThreeDotsLabs/watermill-amqp/)
- Bolt Pub/Sub [(`github.com/ThreeDotsLabs/watermill-bolt`)](https://github.com/ThreeDotsLabs/watermill-bolt/)
- Firestore Pub/Sub [(`github.com/ThreeDotsLabs/watermill-firestore`)](https://github.com/ThreeDotsLabs/watermill-firestore/)
- Google Cloud Pub/Sub [(`github.com/ThreeDotsLabs/watermill-googlecloud`)](https://github.com/ThreeDotsLabs/watermill-googlecloud/)
- HTTP Pub/Sub [(`github.com/ThreeDotsLabs/watermill-http`)](https://github.com/ThreeDotsLabs/watermill-http/)
- io.Reader/io.Writer Pub/Sub [(`github.com/ThreeDotsLabs/watermill-io`)](https://github.com/ThreeDotsLabs/watermill-io/)
- Kafka Pub/Sub [(`github.com/ThreeDotsLabs/watermill-kafka/v2`)](https://github.com/ThreeDotsLabs/watermill-kafka/)
- NATS Pub/Sub [(`github.com/ThreeDotsLabs/watermill-nats`)](https://github.com/ThreeDotsLabs/watermill-nats/)
- Redis Stream Pub/Sub [(`github.com/ThreeDotsLabs/watermill-redisstream`)](https://github.com/ThreeDotsLabs/watermill-redisstream/)
- SQL Pub/Sub [(`github.com/ThreeDotsLabs/watermill-sql/v2`)](https://github.com/ThreeDotsLabs/watermill-sql/)


All Pub/Subs implementation documentation can be found in the [documentation](https://watermill.io/pubsubs/).

## Unofficial libraries

Can't find your favorite Pub/Sub or library integration? Check [Awesome Watermill](https://watermill.io/docs/awesome/).

If you know another library or are an author of one, please [add it to the list](https://github.com/ThreeDotsLabs/watermill/edit/master/docs/content/docs/awesome.md).

## Contributing

Please check our [contributing guide](CONTRIBUTING.md).

## Stability

Watermill v1.0.0 has been released and is production-ready. The public API is stable and will not change without changing the major version.

To ensure that all Pub/Subs are stable and safe to use in production, we created a [set of tests](https://github.com/ThreeDotsLabs/watermill/blob/master/pubsub/tests/test_pubsub.go#L34) that need to pass for each of the implementations before merging to master.
All tests are also executed in [*stress*](https://github.com/ThreeDotsLabs/watermill/blob/master/pubsub/tests/test_pubsub.go#L171) mode - that means that we are running all the tests **20x** in parallel.

All tests are run with the race condition detector enabled (`-race` flag in tests).

For more information about debugging tests, you should check [tests troubleshooting guide](http://watermill.io/docs/troubleshooting/#debugging-pubsub-tests).

## Benchmarks

Initial tools for benchmarking Pub/Subs can be found in [watermill-benchmark](https://github.com/ThreeDotsLabs/watermill-benchmark).

All benchmarks are being done on a single 16 CPU VM instance, running one binary and dependencies in Docker Compose.

These numbers are meant to serve as a rough estimate of how fast messages can be processed by different Pub/Subs.
Keep in mind that the results can be vastly different, depending on the setup and configuration (both much lower and higher).

Here's the short version for message size of 16 bytes.

| Pub/Sub              | Publish (messages / s) | Subscribe (messages / s) |
| -------------------- | ---------------------- | ------------------------ |
| Kafka (one node)     | 63,506                 | 110,811                  |
| Kafka (5 nodes)      | 70,252                 | 117,529                  |
| NATS                 | 76,208                 | 38,169                   |
| SQL (MySQL)          | 7,299                  | 154                      |
| SQL (PostgreSQL)     | 4,142                  | 98                       |
| Google Cloud Pub/Sub | 7,416                  | 39,591                   |
| AMQP                 | 2,408                  | 10,608                   |
| GoChannel            | 272,938                | 101,371                  |

## Support

If you didn't find the answer to your question in [the documentation](https://watermill.io/), feel free to ask us directly!

Please join us on the `#watermill` channel on the [Three Dots Labs Discord](https://discord.gg/QV6VFg4YQE).

Every bit of feedback is very welcome and appreciated. Please submit it using [the survey](https://www.surveymonkey.com/r/WZXD392).

## Why the name?

It processes streams!

## License

[MIT License](./LICENSE)
