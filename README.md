# Watermill
[![Build Status](https://travis-ci.org/ThreeDotsLabs/watermill.svg?branch=master)](https://travis-ci.org/ThreeDotsLabs/watermill)
[![Go Report Card](https://goreportcard.com/badge/github.com/ThreeDotsLabs/watermill)](https://goreportcard.com/report/github.com/ThreeDotsLabs/watermill)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2FThreeDotsLabs%2Fwatermill.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2FThreeDotsLabs%2Fwatermill?ref=badge_shield)


Watermill is a Go library for working efficiently with message streams. It is intended
for building event driven applications, enabling event sourcing, RPC over messages,
sagas and basically whatever else comes to your mind. You can use conventional pub/sub
implementations like Kafka or RabbitMQ, but also HTTP or MySQL binlog if that fits your use case.

**Note:** Watermill is still under heavy development. The public API can change before the 1.0.0 release.

Documentation: https://watermill.io/

Getting started guide: https://watermill.io/docs/getting-started/

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

## Features

* **Easy** to understand (see examples below).
* **Universal** - event-driven architecture, messaging, stream processing, CQRS - use it for whatever you need.
* **Fast** - *(benchmarks coming soon)*
* **Flexible** with middlewares and plugins.
* **Resilient** - using proven technologies and passing stress tests *(results coming soon)*.

## Pub/Subs

All publishers and subscribers have to implement an interface:

```go
type Publisher interface {
	Publish(topic string, messages ...*Message) error
	Close() error
}

type Subscriber interface {
	Subscribe(topic string) (chan *Message, error)
	Close() error
}
```

All Pub/Subs implementation can be found in the [documentation](https://watermill.io/docs/pub-sub-implementations/).

## Examples
* [Your first app](_examples/your-first-app) - start here!
* [Simple application with publisher and subscriber](_examples/simple-app)
* [HTTP to Kafka](_examples/http-to-kafka)

## Contributing

All contributions are very much welcome. If you'd like to help with Watermill development,
please see [open issues](https://github.com/ThreeDotsLabs/watermill/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+)
and submit your pull request via GitHub.

## Support

If you didn't found answer to your question in [the documentation](https://watermill.io/), feel free to ask!

Please join us on the `#watermill` channel on the [Gophers slack](https://gophers.slack.com/): You can get invite [here](https://gophersinvite.herokuapp.com/).

## Why the name?

It processes streams!

## License

[MIT License](./LICENSE)


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2FThreeDotsLabs%2Fwatermill.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2FThreeDotsLabs%2Fwatermill?ref=badge_large)
