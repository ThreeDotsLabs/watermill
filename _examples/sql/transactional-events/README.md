# Transactional Events

This example shows how to use the SQL Subscriber from [SQL Pub/Sub](https://github.com/ThreeDotsLabs/watermill-sql).

The SQL subscriber listens for new records on a MySQL table. Each new record will result in a new event published
on Gochannel Publisher.

This technique is useful for saving domain events in transaction with the aggregate.

Note that gochannel is used to keep the example simple and any other subscriber can be used instead.

The example uses `DefaultSchema`, but you can define your own table definition and queries.
See [SQL Pub/Sub documentation](https://watermill.io/pubsub/sql) for details.

## Requirements

To run this example you will need Docker and docker-compose installed. See installation guide at https://docs.docker.com/compose/install/

## Running

```bash
docker-compose up
```
