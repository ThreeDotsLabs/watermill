+++
title = "SQLite"
description = "Pub/Sub based on SQLite."
date = 2025-05-08T11:30:00+02:00
bref = "Pub/Sub based on SQLite."
weight = 121
+++

[todo - some description]

## Vanilla ModernC Driver vs Advanced ZombieZen Driver

The ModernC driver is compatible with the Golang standard library SQL package. It works without CGO. Has fewer dependencies than the ZombieZen variant.

The ZombieZen driver abandons the standard Golang library SQL conventions in favor of [the more orthogonal API and higher performance potential](https://crawshaw.io/blog/go-and-sqlite). Under the hood, it uses ModernC SQLite3 implementation and does not need CGO. Advanced SQLite users might prefer this driver.
It is about **6 times faster** than the ModernC variant. It is currently more stable due to lower level control. It is faster than even the CGO SQLite variants on standard library interfaces, and with some tuning should become the absolute speed champion of persistent message brokers over time. Tuned SQLite is [~35% faster](https://sqlite.org/fasterthanfs.html) than the Linux file system.

### Characteristics

| Feature             | Implements | Note                                              |
|---------------------|------------|---------------------------------------------------|
| ConsumerGroups      | yes        | See `ConsumerGroupMatcher` in `SubscriberOptions` |
| ExactlyOnceDelivery | false      |                                                   |
| GuaranteedOrder     | yes        |                                                   |
| Persistent          | yes        |                                                   |

## Vanilla ModernC Driver

### Installation

```sh
go get -u github.com/dkotik/watermillsqlite/wmsqlitemodernc
```

### Usage 

```go
import (
	"database/sql"
	"github.com/dkotik/watermillsqlite/wmsqlitemodernc"
	_ "modernc.org/sqlite"
)

db, err := sql.Open("sqlite", ":memory:?journal_mode=WAL&busy_timeout=1000&cache=shared")
if err != nil {
	panic(err)
}
// limit the number of concurrent connections to one
// this is a limitation of `modernc.org/sqlite` driver
db.SetMaxOpenConns(1)
defer db.Close()

pub, err := wmsqlitemodernc.NewPublisher(db, wmsqlitemodernc.PublisherOptions{
	InitializeSchema: true, // create tables for used topics
})
if err != nil {
	panic(err)
}
sub, err := wmsqlitemodernc.NewSubscriber(db, wmsqlitemodernc.SubscriberOptions{
	InitializeSchema: true, // create tables for used topics
})
if err != nil {
	panic(err)
}
// ... follow guides on <https://watermill.io>
```

### Configuration

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/publisher.go" first_line_contains="type PublisherOptions struct" last_line_contains="}" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/subscriber.go" first_line_contains="type SubscriberOptions struct" last_line_contains="}" %}}

### Publishing

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/publisher.go" first_line_contains="func NewPublisher" last_line_contains="func NewPublisher" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/publisher.go" first_line_contains="// Publish " last_line_contains="func (p *publisher) Publish" %}}

#### Publishing in transaction

[todo]

### Subscribing

[todo]

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitemodernc/subscriber.go" first_line_contains="func NewSubscriber" last_line_contains="func NewSubscriber" %}}

Example:

[todo]

## Advanced ZombieZen Driver

### Installation

```sh
go get -u github.com/dkotik/watermillsqlite/wmsqlitezombiezen
```

### Usage

```go
import "github.com/dkotik/watermillsqlite/wmsqlitezombiezen"

// &cache=shared is critical, see: https://github.com/zombiezen/go-sqlite/issues/92#issuecomment-2052330643
connectionDSN := ":memory:?journal_mode=WAL&busy_timeout=1000&cache=shared")
conn, err := sqlite.OpenConn(connectionDSN)
if err != nil {
	panic(err)
}
defer conn.Close()

pub, err := wmsqlitezombiezen.NewPublisher(conn, wmsqlitezombiezen.PublisherOptions{
	InitializeSchema: true, // create tables for used topics
})
if err != nil {
	panic(err)
}
sub, err := wmsqlitezombiezen.NewSubscriber(connectionDSN, wmsqlitezombiezen.SubscriberOptions{
	InitializeSchema: true, // create tables for used topics
})
if err != nil {
	panic(err)
}
// ... follow guides on <https://watermill.io>
```

### Configuration

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/publisher.go" first_line_contains="type PublisherOptions struct" last_line_contains="}" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/subscriber.go" first_line_contains="type SubscriberOptions struct" last_line_contains="}" %}}

### Publishing

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/publisher.go" first_line_contains="func NewPublisher" last_line_contains="func NewPublisher" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/publisher.go" first_line_contains="// Publish " last_line_contains="func (p *publisher) Publish" %}}

#### Publishing in transaction

[todo]

### Subscribing

[todo]

{{% load-snippet-partial file="src-link/watermill-sqlite/wmsqlitezombiezen/subscriber.go" first_line_contains="func NewSubscriber" last_line_contains="func NewSubscriber" %}}

Example:

[todo]

## Similar Projects

- <https://github.com/davidroman0O/watermill-comfymill>
- <https://github.com/walterwanderley/watermill-sqlite>

## Caveats

### SQLite limitations

SQLite3 does not support querying `FOR UPDATE`, which is used for row locking when subscribers in the same consumer group read an event batch in official Watermill SQL PubSub implementations. Current architectural decision is to lock a consumer group offset using `unixepoch()+lockTimeout` time stamp. While one consumed message is processing per group, the offset lock time is extended by `lockTimeout` periodically by `time.Ticker`. If the subscriber is unable to finish the consumer group batch, other subscribers will take over the lock as soon as the grace period runs out. A time lock fulfills the role of a traditional database network timeout that terminates transactions when its client disconnects.