# Persistent Event Log (Google Cloud Pub/Sub to MySQL)

This example shows how to use the SQL Publisher from [SQL Pub/Sub](https://github.com/ThreeDotsLabs/watermill-sql).

## Background

Some PubSubs (e.g. Kafka) come with support for storing processed messages, possibly even with no expiration date.
This can be useful for audit purposes or to reply selected messages again in the future. But what if you'd like to use
a PubSub that offers no storage and an event log is needed?

## Solution

Plugging a pair of a publisher and a subscriber into Watermill's `Router` can work as a proxy from one PubSub to another.
To ensure that events are written to a persistent storage, you can use the SQL publisher.

Google Cloud Pub/Sub subscriber consumes events from a topic and inserts them into a MySQL table. While this particular 
PubSub doesn't guarantee proper order of messages, you can use `OccurredAt` field of the payload for sorting.
Google Cloud Pub/Sub is used just as an example and any other subscriber can be used instead.

The example uses `DefaultMySQLSchema`, but you can define your own table definition and queries.
See [SQL Pub/Sub documentation](https://watermill.io/pubsubs/sql) for details.

## Requirements

To run this example you will need Docker and docker-compose installed. See installation guide at https://docs.docker.com/compose/install/

## Running

```bash
docker-compose up
```

After few seconds, some events should be saved in the table:

```
docker-compose exec mysql mysql -e 'select * from watermill.watermill_events;'
+--------+--------------------------------------+---------------------+---------+----------+
| offset | uuid                                 | created_at          | payload | metadata |
+--------+--------------------------------------+---------------------+---------+----------+
|      1 | 2faf6a14-f52a-4d6c-a4be-7355db428be1 | 2019-08-17 12:23:35 | {...}   | {}       |
|      2 | cccfe73c-1968-4e20-b8b7-3763f68dc60b | 2019-08-17 12:23:35 | {...}   | {}       |
|      3 | e8585f50-5e38-4569-bd93-fe4f6e960e61 | 2019-08-17 12:23:36 | {...}   | {}       |
|      4 | 2d364b7e-fc4d-459c-972a-8859c8f1a655 | 2019-08-17 12:23:37 | {...}   | {}       |
|      5 | 3b9da717-aad8-4e4b-a6e2-2d7040454015 | 2019-08-17 12:23:38 | {...}   | {}       |
|      6 | 5c07a2e7-464e-4ffb-8ada-0e2f02e48111 | 2019-08-17 12:23:39 | {...}   | {}       |
|      7 | 60a30b9e-6a40-4f41-94f9-8e7c8a38a998 | 2019-08-17 12:23:40 | {...}   | {}       |
|      8 | 3d28a15a-7448-4535-9b79-27111579e341 | 2019-08-17 12:23:41 | {...}   | {}       |
|      9 | 3c448aff-6bdd-4fc4-9b56-8bacab0b2746 | 2019-08-17 12:23:42 | {...}   | {}       |
|     10 | 9b56ca67-4c47-4bcd-931f-86f9af62775d | 2019-08-17 12:23:43 | {...}   | {}       |
+--------+--------------------------------------+---------------------+---------+----------+
```
