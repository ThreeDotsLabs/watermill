# Sending webhooks (Kafka to HTTP)

This example showcases the use of the **HTTP Publisher** to call webhooks with HTTP POST requests. It consists of three services:

1. `producer` publishes messages on Kafka. The messages come in three varieties: `Foo`, `Bar`, and `Baz`. The event type is encoded in metadata, under the key `event_type`.
1. `webhook_server` is a HTTP server that listens for requests and prints the path, method, and payload on stdout.
1. `router` consumes the Kafka messages, and uses the HTTP producer to send requests to `webhook_server`. To illustrate how one message can spawn multiple webhooks, the following paths are called based on `event_type`:
    1. `/foo` for events of type `Foo`
    1. `/foo_or_bar` for events of type `Foo` or `Bar`
    1. `/all` for all events.

Additionally, services `zookeper` and `kafka` are present to provide backend for the Kafka producer and subscriber.

## Requirements

To run this example you will need Docker and docker-compose installed. See installation guide at https://docs.docker.com/compose/install/

## Running

To run all services, execute:

```
docker-compose up
```

To filter messages from a specific service, execute:

```
docker-compose logs [-f] {service}
```

in a separate terminal window while the services are running. Use the `-f` flag to emulate `tail -f` behavior, i.e. follow the output.
