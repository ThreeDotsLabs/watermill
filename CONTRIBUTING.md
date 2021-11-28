# Contributors guide v0.1

## How can I help?

We are always happy to help you in contributing to Watermill. If you have any ideas, please let us know on our [Discord server](https://watermill.io/support/).

There are multiple ways in which you can help us.

### Existing issues

You can pick one of the existing issues. Most of the issues should have an estimation (S - small, M - medium, L - large).

- [Good first issues list](https://github.com/ThreeDotsLabs/watermill/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) - simple issues to begin with
- [Help wanted issues list](https://github.com/ThreeDotsLabs/watermill/issues?q=is%3Aissue+is%3Aopen+label%3A%22help+wanted%22) - tasks that are already more or less clear, and you can start to implement them pretty quickly

### New Pub/Sub implementations

If you have an idea to create a Pub/Sub based on some technology and it is not listed yet in our issues (because we don't know it, or it is just some crazy idea, like physical mail based Pub/Sub), feel free to add your own implementation.
You can do it in your private repository or if you want, we can move it to `ThreeDotsLabs/watermill-[name]`.

*Please keep in mind that you will not be able to push changes directly to the master branch in a project in our organization*.

When adding a new Pub/Sub implementation, you should start with this guide: [https://watermill.io/docs/pub-sub-implementing/](https://watermill.io/docs/pub-sub-implementing/).

### New ideas

If you have any idea that is not covered in the issues list, please post a new issue describing it. 
It's recommended to discuss your idea on [Discord](https://discord.gg/QV6VFg4YQE)/GitHub before creating production-ready implementation - in some situations, it may save a lot of your time before implementing something that can be simplified or done more easily. :)

In general, it's helpful to discuss a Proof of Concept to align with the idea.

## Local development

Makefile and docker-compose (for Pub/Subs) are your friends. You can run all tests locally (they are running in CI in the same way).

Useful commands:
- `make up` - docker-compose up
- `make test` - tests
- `make test_short` - run short tests (useful to perform a very fast check after changes)
- `make fmt` - do goimports

## Code standards

- you should run `make fmt`
- [CodeReviewComments](https://github.com/golang/go/wiki/CodeReviewComments)
- [Effective Go](https://golang.org/doc/effective_go.html)
- SOLID
- code should be open for configuration and not coupled to any serialization method (for example: [AMQP marshaler](https://github.com/ThreeDotsLabs/watermill-amqp/blob/master/pkg/amqp/marshaler.go), [AMQP Config](https://github.com/ThreeDotsLabs/watermill-amqp/blob/master/pkg/amqp/config.go)
