+++
title = "SQLite"
description = "A lightweight, file-based SQL database engine"
date = 2025-05-08T11:30:00+02:00
bref = "A lightweight, file-based SQL database engine"
weight = 121
+++

**Beta Version Warning: this Pub/Sub is stable, but it has not been widely tested in production environments. It may be sensitive to certain edge cases and combinations of configuration parameters.**

SQLite is a C-language library that implements a small, fast, self-contained, high-reliability, full-featured SQL database engine. Our SQLite Pub/Sub implementation provides two **CGO-free** driver variants optimized for different use cases. Both drivers use pure Go implementations of SQLite, enabling cross-compilation and avoiding CGO dependencies while maintaining full SQLite functionality.

SQLite Pub/Subs provide the easiest way to publish and process events durably, since you do not have to set up or manage a separate database. The database is just a file on disk. Some cloud compute providers offer distributed SQLite clusters, which can provide both durability and unmatched read performance. Tuned SQLite is [~35% faster](https://sqlite.org/fasterthanfs.html) than the Linux file system.

You can find a fully functional example with SQLite in the [Watermill examples](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/pubsubs/sqlite).

## Vanilla ModernC Driver vs Advanced ZombieZen Driver

The **ModernC driver** is compatible with the Golang standard library SQL package and works without CGO. It has fewer dependencies than the ZombieZen variant and uses the `modernc.org/sqlite` pure Go SQLite implementation.

The **ZombieZen driver** abandons the standard Golang library SQL conventions in favor of [the more orthogonal API and higher performance potential](https://crawshaw.io/blog/go-and-sqlite). Under the hood, it also uses the ModernC SQLite3 implementation and does not need CGO. Advanced SQLite users might prefer this driver for its performance benefits.
It is about **6 times faster** than the ModernC variant. It is currently more stable due to lower level control. It is faster than even the CGO SQLite variants on standard library interfaces, and with some tuning should become the absolute speed champion of persistent message brokers over time.

### Characteristics

| Feature             | Implements | Note                                              |
|---------------------|------------|---------------------------------------------------|
| ConsumerGroups      | yes        | See `ConsumerGroupMatcher` in `SubscriberOptions` |
| ExactlyOnceDelivery | no         |                                                   |
| GuaranteedOrder     | yes        |                                                   |
| Persistent          | yes        |                                                   |

## Vanilla ModernC Driver

### Installation

```bash
go get github.com/ThreeDotsLabs/watermill-sqlite/wmsqlitemodernc@latest
```

### Usage

{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite/main.go" first_line_contains="import (" last_line_contains="}" padding_after="1" %}}

### Configuration

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/publisher.go" first_line_contains="type PublisherOptions struct" last_line_contains="}" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/subscriber.go" first_line_contains="type SubscriberOptions struct" last_line_contains="}" %}}

### Publishing

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/publisher.go" first_line_contains="// NewPublisher" last_line_contains="func NewPublisher" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/publisher.go" first_line_contains="// Publish " last_line_contains="func (p *publisher) Publish" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite/main.go" first_line_contains="publisher, err := wmsqlitemodernc.NewPublisher(" last_line_contains="panic(err)" padding_after="1" %}}

{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite/main.go" first_line_contains="func publishMessages(" last_line_contains="panic(err)" padding_after="1" %}}

#### Publishing in transaction

{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite/transaction.go" first_line_contains="import (" last_line_contains="}" padding_after="1" %}}

### Subscribing

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/subscriber.go" first_line_contains="// NewSubscriber" last_line_contains="func NewSubscriber" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite/main.go" first_line_contains="subscriber, err := wmsqlitemodernc.NewSubscriber(" last_line_contains="panic(err)" padding_after="1" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/subscriber.go" first_line_contains="// Subscribe " last_line_contains="func (s *subscriber) Subscribe" %}}

## Advanced ZombieZen Driver

### Installation

```bash
go get -u github.com/ThreeDotsLabs/watermill-sqlite/wmsqlitezombiezen@latest
```

### Usage

{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite-zombiezen/main.go" first_line_contains="import (" last_line_contains="}" padding_after="1" %}}

### Configuration

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/publisher.go" first_line_contains="type PublisherOptions struct" last_line_contains="}" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/subscriber.go" first_line_contains="type SubscriberOptions struct" last_line_contains="}" %}}

### Publishing

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/publisher.go" first_line_contains="// NewPublisher" last_line_contains="func NewPublisher" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/publisher.go" first_line_contains="// Publish " last_line_contains="func (p *publisher) Publish" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite-zombiezen/main.go" first_line_contains="publisher, err := wmsqlitezombiezen.NewPublisher(" last_line_contains="panic(err)" padding_after="1" %}}

{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite-zombiezen/main.go" first_line_contains="func publishMessages(" last_line_contains="panic(err)" padding_after="1" %}}

#### Publishing in transaction

{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite-zombiezen/transaction.go" first_line_contains="import (" last_line_contains="}" padding_after="1" %}}

### Subscribing

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/subscriber.go" first_line_contains="// NewSubscriber" last_line_contains="func NewSubscriber" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sqlite-zombiezen/main.go" first_line_contains="subscriber, err := wmsqlitezombiezen.NewSubscriber(" last_line_contains="panic(err)" padding_after="1" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/subscriber.go" first_line_contains="// Subscribe " last_line_contains="func (s *subscriber) Subscribe" %}}

## Marshaler

Watermill's messages are stored in SQLite using JSON serialization. Both drivers use the same marshaling approach - messages are automatically marshaled to and from JSON format when publishing and subscribing.

The default marshaler handles:

- Message payload (stored as JSON blob)
- Message metadata (stored as JSON object)
- Message UUID (stored as TEXT)
- Timestamps for ordering and consumer group management

Both drivers automatically handle message marshaling and unmarshaling, so no custom marshaler configuration is typically required.

## Similar Projects

- <https://github.com/davidroman0O/watermill-comfymill>
- <https://github.com/walterwanderley/watermill-sqlite>

## Caveats

SQLite3 does not support querying `FOR UPDATE`, which is used for row locking when subscribers in the same consumer group read an event batch in official Watermill SQL PubSub implementations. Current architectural decision is to lock a consumer group offset using `unixepoch()+lockTimeout` time stamp. While one consumed message is processing per group, the offset lock time is extended by `lockTimeout` periodically by `time.Ticker`. If the subscriber is unable to finish the consumer group batch, other subscribers will take over the lock as soon as the grace period runs out. A time lock fulfills the role of a traditional database network timeout that terminates transactions when its client disconnects.

All the normal SQLite limitations apply to Watermill. The connections are file handles. Create new connections for concurrent processing. If you must share a connection, protect it with a mutual exclusion lock. If you are writing within a transaction, create a connection for that transaction only.
