+++
title = "SQL"
description = "SQL-based Pub/Sub"
date = 2019-07-06T22:30:00+02:00
bref = "SQL-based Pub/Sub"
weight = -50
type = "docs"
toc = false
+++

### SQL

SQL Pub/Sub executes queries on any SQL database, using it like a messaging system.

While the performance of this approach isn't the best, it fits many use cases, where eventual consistency is acceptable.
It can also be useful for projects that are not using any specialized message queue at the moment, but have access to a SQL database.

The SQL subscriber runs a `SELECT` query within short periods, remembering the position of the last record. If it finds 
any new records, they are returned. One handy use case is consuming events from a database table, that can be later published
on some kind of message queue.

The SQL publisher simply inserts consumed messages into the chosen table. A common approach would be to use it as a persistent
log of events that were published on a queue with short message expiration time.

See also the [SQL example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/pubsubs/sql).

#### Characteristics

| Feature             | Implements | Note |
| ------------------- | ---------- | ---- |
| ConsumerGroups      | yes        | See `ConsumerGroup` in `SubscriberConfig` |
| ExactlyOnceDelivery | yes        | |
| GuaranteedOrder     | yes        | |
| Persistent          | yes        | |

#### Schema

SQL Pub/Sub uses user-defined schema to handle select and insert queries. You need to implement `SchemaAdapter` and pass
it to `SubscriberConfig` or `PublisherConfig`.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/schema_adapter_mysql.go" first_line_contains="// DefaultMySQLSchema" last_line_contains="type DefaultMySQLSchema" %}}
{{% /render-md %}}

There is `DefaultSchema` defined for most common use case (storing events in a table). You can base your schema on this
one, extending only chosen methods.

##### Extending schema

Consider an example project, where you're fine with using the default schema, but would like to use `BINARY(16)` for storing
the `uuid` column, instead of `VARCHAR(36)`. In that case, you have to define two methods:

* `SchemaInitializingQueries` that creates the table.
* `UnmarshalMessage` method that produces a `Message` from the database record.

Note that you don't have to use the initialization queries provided by Watermill. They will be run only if you set the
`InitializeSchema` field to `true` in the config. Otherwise, you can use your own solution for database migrations.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/schema_adapter_mysql.go" first_line_contains="// DefaultMySQLSchema" last_line_contains="type DefaultMySQLSchema" %}}
{{% /render-md %}}

#### Configuration

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/publisher.go" first_line_contains="type PublisherConfig struct" last_line_contains="}" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="}" %}}
{{% /render-md %}}

### Publishing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/publisher.go" first_line_contains="func NewPublisher" last_line_contains="func NewPublisher" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/publisher.go" first_line_contains="// Publish " last_line_contains="func (p *Publisher) Publish" %}}
{{% /render-md %}}

#### Transactions

If you need to publish messages within a database transaction, you have to pass a `*sql.Tx` in the `NewPublisher`
constructor. You have to create one publisher for each transaction. 

Example:
{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/real-world-examples/transactional-events/main.go" first_line_contains="func simulateEvents" last_line_contains="return pub.Publish(" padding_after="3" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/subscriber.go" first_line_contains="func NewSubscriber" last_line_contains="func NewSubscriber" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="subscriber, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/subscriber.go" first_line_contains="func (s *Subscriber) Subscribe" last_line_contains="func (s *Subscriber) Subscribe" %}}
{{% /render-md %}}

#### Offsets Adapter

The logic for storing offsets of messages is provided by the `OffsetsAdapter`. The MySQL variant is defined and ready to use.

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/offsets_adapter.go" first_line_contains="type OffsetsAdapter" %}}
{{% /render-md %}}
