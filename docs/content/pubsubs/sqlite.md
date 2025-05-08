+++
title = "SQLite"
description = "Pub/Sub based on SQLite."
date = 2025-05-08T11:30:00+02:00
bref = "Pub/Sub based on SQLite."
weight = 120
+++

[todo]

## Vanilla ModernC Driver vs Advanced ZombieZen Driver

[todo]

## Installation

```bash
go get github.com/ThreeDotsLabs/watermill-sqliteite
```

### Characteristics

| Feature             | Implements | Note                                                                          |
|---------------------|------------|-------------------------------------------------------------------------------|
| ConsumerGroups      | yes        | See `ConsumerGroup` in `SubscriberConfig` (not supported by the queue schema) |
| ExactlyOnceDelivery | yes        | Just for MySQL implementation                                                 |
| GuaranteedOrder     | yes        |                                                                               |
| Persistent          | yes        |                                                                               |

# Vanilla ModernC Driver

### Configuration

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/publisher.go" first_line_contains="type PublisherOptions struct" last_line_contains="}" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/subscriber.go" first_line_contains="type SubscriberOptions struct" last_line_contains="}" %}}

## Publishing

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/publisher.go" first_line_contains="func NewPublisher" last_line_contains="func NewPublisher" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/publisher.go" first_line_contains="// Publish " last_line_contains="func (p *publisher) Publish" %}}

### Publishing in transaction

[todo]

## Subscribing

[todo]

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/subscriber.go" first_line_contains="func NewSubscriber" last_line_contains="func NewSubscriber" %}}

Example:

[todo]

# Advanced ZombieZen Driver