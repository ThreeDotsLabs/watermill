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

SQL Pub/Sub runs queries on any SQL database. While the performance of this approach isn't the best, it fits many use cases,
where eventual consistency is acceptable.

The SQL subscriber runs a `SELECT` query within short periods, remembering the position of the last record. If it finds 
any new records, they are returned. One handy use case is inserting events into a database table, that will be later consumed
by Watermill and published on some kind of message queue.

The SQL publisher simply inserts consumed messages into chosen table. It may be used as a persistent log of events published on
shortly-lived queue.

#### Characteristics

| Feature             | Implements | Note |
| ------------------- | ---------- | ---- |
| ConsumerGroups      | yes        | See `ConsumerGroup` in `SubscriberConfig` |
| ExactlyOnceDelivery | yes        | |
| GuaranteedOrder     | yes        | |
| Persistent          | yes        | |

#### Configuration

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/publisher.go" first_line_contains="type PublisherConfig struct" last_line_contains="}" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/subscriber.go" first_line_contains="type SubscriberConfig struct" last_line_contains="}" %}}
{{% /render-md %}}

#### Connecting

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/publisher.go" first_line_contains="func NewPublisher" last_line_contains="func NewPublisher" %}}

Example:
{{% load-snippet-partial file="content/docs/getting-started/sql/main.go" first_line_contains="publisher, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/subscriber.go" first_line_contains="func NewSubscriber" last_line_contains="func NewSubscriber" %}}

Example:
{{% load-snippet-partial file="content/docs/getting-started/sql/main.go" first_line_contains="subscriber, err :=" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

#### Publishing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/publisher.go" first_line_contains="// Publish " last_line_contains="func (p *Publisher) Publish" %}}
{{% /render-md %}}

#### Subscribing

{{% render-md %}}
{{% load-snippet-partial file="src-link/watermill-sql/pkg/sql/subscriber.go" first_line_contains="func (s *Subscriber) Subscribe" last_line_contains="func (s *Subscriber) Subscribe" %}}
{{% /render-md %}}

#### Schema

SQL Pub/Sub uses user-defined schema to handle select and insert queries. You need to implement [SchemaAdapter](https://github.com/ThreeDotsLabs/watermill-sql/blob/master/pkg/sql/schema_adapter.go)
and pass it to `SubscriberConfig` or `PublisherConfig`. There is `DefaultSchema` defined for most common use case
(storing events in a table).

#### Offsets Schema
