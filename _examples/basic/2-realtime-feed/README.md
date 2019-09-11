# Realtime Feed

This example features a very busy blogging platform, with thousands of messages showing up on your feed.

There are two separate applications (microservices) integrating over a Kafka topic. The [`producer`](producer/) generates
thousands of "posts" and publishes them to the topic. The [`consumer`](consumer/) subscribes to this topic and 
displays each post on the standard output.

The consumer has a throttling middleware enabled, so you have a chance to actually read the posts.

To understand the background and internals, see [getting started guide](https://watermill.io/docs/getting-started/).

## Requirements

To run this example you will need Docker and docker-compose installed. See the [installation guide](https://docs.docker.com/compose/install/).

## Running

```bash
docker-compose up
```

You should see the live feed of posts on the standard output.

## Exercises

1. Peek into the posts counter published on `posts_count` topic.

```
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic posts_count
```

2. Add a persistent storage for incoming posts in the consumer service, instead of displaying them.
   Consider using the [SQL Publisher](https://github.com/ThreeDotsLabs/watermill-sql).
