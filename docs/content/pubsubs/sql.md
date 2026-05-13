+++
title = "SQL (PostgreSQL, MySQL)"
description = "Pub/Sub based on MySQL or PostgreSQL."
date = 2019-07-06T22:30:00+02:00
bref = "Pub/Sub based on MySQL or PostgreSQL."
weight = 120
+++

SQL Pub/Sub executes queries on any SQL database, using it like a messaging system. At the moment, **MySQL** and **PostgreSQL** are supported.
It be useful for projects that are not using any specialized message queue at the moment, but have access to a SQL database.

If you are looking for SQLite Pub/Sub, check out the [SQLite Pub/Sub](/pubsubs/sqlite/) documentation.

The SQL subscriber runs a `SELECT` query within short periods, remembering the position of the last record. If it finds
any new records, they are returned. One handy use case is consuming events from a database table, that can be later published
on some kind of message queue.

The SQL publisher simply inserts consumed messages into the chosen table. A common approach would be to use it as a persistent
log of events that were published on a queue with short message expiration time.

SQL Pub/Sub is also a good choice for implementing Outbox pattern with [Forwarder](/docs/forwarder/) component.

See also the [SQL example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/pubsubs/sql).

## Installation

```bash
go get github.com/ThreeDotsLabs/watermill-sql/v4
```

### Characteristics

| Feature             | Implements | Note                                                                          |
|---------------------|------------|-------------------------------------------------------------------------------|
| ConsumerGroups      | yes        | See `ConsumerGroup` in `SubscriberConfig` (not supported by the queue schema) |
| ExactlyOnceDelivery | yes        |
| GuaranteedOrder     | yes        |                                                                               |
| Persistent          | yes        |                                                                               |

### Schema

SQL Pub/Sub uses user-defined schema to handle select and insert queries. You need to implement `SchemaAdapter` and pass
it to `SubscriberConfig` or `PublisherConfig`.

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/schema_adapter_mysql.go" first_line_contains="// DefaultMySQLSchema" last_line_contains="type DefaultMySQLSchema" %}}

There is a default schema provided for each supported engine (`DefaultMySQLSchema` and `DefaultPostgreSQLSchema`).
It supports the most common use case (storing events in a table). You can base your schema on one of these, extending only chosen methods.

#### Extending schema

Consider an example project, where you're fine with using the default schema, but would like to use `BINARY(16)` for storing
the `uuid` column, instead of `VARCHAR(36)`. In that case, you have to define two methods:

* `SchemaInitializingQueries` that creates the table.
* `UnmarshalMessage` method that produces a `Message` from the database record.

Note that you don't have to use the initialization queries provided by Watermill. They will be run only if you set the
`InitializeSchema` field to `true` in the config. Otherwise, you can use your own solution for database migrations.

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/schema_adapter_mysql.go" first_line_contains="// DefaultMySQLSchema" last_line_contains="type DefaultMySQLSchema" %}}

### Configuration

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/publisher.go" first_line_contains="type PublisherConfig struct" last_line_contains="}" %}}

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="}" %}}

## Publishing

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/publisher.go" first_line_contains="func NewPublisher" last_line_contains="func NewPublisher" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/publisher.go" first_line_contains="// Publish " last_line_contains="func (p *Publisher) Publish" %}}

### Transactions

If you need to publish messages within a database transaction, you have to pass a `*sql.Tx` in the `NewPublisher`
constructor. You have to create one publisher for each transaction.

Example:
{{% load-snippet-partial file="src-link/_examples/real-world-examples/transactional-events/main.go" first_line_contains="func simulateEvents" last_line_contains="return pub.Publish(" padding_after="3" %}}

## Subscribing

To create a subscriber, you need to pass not only proper schema adapter, but also an offsets adapter.

* For MySQL schema use `DefaultMySQLOffsetsAdapter`
* For PostgreSQL schema use `DefaultPostgreSQLOffsetsAdapter`

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/subscriber.go" first_line_contains="func NewSubscriber" last_line_contains="func NewSubscriber" %}}

Example:
{{% load-snippet-partial file="src-link/_examples/pubsubs/sql/main.go" first_line_contains="subscriber, err :=" last_line_contains="panic(err)" padding_after="1" %}}

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/subscriber.go" first_line_contains="func (s *Subscriber) Subscribe" last_line_contains="func (s *Subscriber) Subscribe" %}}

## Offsets Adapter

The logic for storing offsets of messages is provided by the `OffsetsAdapter`. If your schema uses auto-incremented integer as the row ID,
it should work out of the box with default offset adapters.

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/offsets_adapter.go" first_line_contains="type OffsetsAdapter" %}}

## Queue

Instead of the default Pub/Sub schema, you can use the *queue* schema and offsets adapters.

It's a simpler schema that doesn't support consumer groups.
However, it has other advantages.

It lets you specify a custom `WHERE` clause for getting the messages.
You can use it to filter messages by some condition in the payload or in the metadata.

Additionally, you can choose to delete messages from the table after they are acknowledged.
Thanks to this, the table doesn't grow in size with time.

This schema is supported by both PostgreSQL and MySQL.
The example below is based on PostgreSQL, but the same approach can be used with MySQL. 

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/queue_schema_adapter_postgresql.go" first_line_contains="// PostgreSQLQueueSchema" last_line_contains="}" %}}

{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/queue_offsets_adapter_postgresql.go" first_line_contains="// PostgreSQLQueueOffsetsAdapter" last_line_contains="}" %}}

## Caveats

### Using last processed transaction ID in PostgreSQL to ensure no messages are lost

In some cases, PostgreSQL `SERIAL` is not incremental.
The `SERIAL` value is generated while the transaction is in progress, not when it is committed.
If transactions are committed in a different order than they were started, message offsets based on `SERIAL` values will not be incremental.

To keep storing acknowledgment information efficient, Watermill keeps only the last message's acknowledgment information.
To ensure no messages are missed when a message order is not kept, Watermill also uses the transaction ID to ensure no message is lost.
For more details, see [Watermill#311](https://github.com/ThreeDotsLabs/watermill/issues/311).

It is important to note that very long-running transactions may result in delayed message delivery.
For instance, if a transaction is running for an hour, no messages will be delivered until the transaction is committed.
While we do not recommend the use of such long transactions, **if they are necessary, we advise the use of the [Queue schema adapter](#queue), which does not depend on the transaction ID.**
You have nothing to worry about if you don't have such long transactions.

If you are migrating your data to a new database, you may need to set `last_processed_transaction_id` in your offsets table.
