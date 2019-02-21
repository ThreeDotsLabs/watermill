# CLI Wrapper for Watermill

This is a CLI wrapper for Watermill. It has two basic functionalities: producing and consuming messages on the following
Pub/Subs:

1. Kafka
2. Google Cloud Pub/Sub
3. RabbitMQ

## Consume mode

In consume mode, the tool subscribes to a topic/queue/subscription (delete as appropriate) and prints the messages
in a simplified format to the standard output:

```
[yyyy-mm-dd hh:mm:ss.ssssssss] topic: message payload
```

Other outputs, for example ones that preserve UUIDs or metadata, are easily attainable by modification
of the marshaling function of the `io.Publisher` of the `consumeCmd`.

## Produce mode

In produce mode, subsequent lines of data from the standard input are transformed into messages outgoing to the requested
provider's topic/exchange. 

The message's payload is set to the line from stdin, the UUID is auto-generated and the metadata is empty.

Similarly, the contents of the message could be parsed differently from stdin, by modification
of the unmarshaling function of the `io.Subscriber` of the `produceCmd`.

## Usage

The basic syntax of the tool is:

```bash
watermill-cli <provider> <command>
```

with the appropriate flags regulating the specific behaviour of each command.

`command` is usually one of `produce` or `consume`, but some providers may handle additional commands
that are specific for them.

The flags are context-specific, so the best way to find out about them is to use the `-h` flag and study 
which flags are available/required for the specific context and act accordingly.

## Additional functionalities

### Google Cloud Pub/Sub

#### Adding/Removing subscriptions

The CLI tool allows to create/remove subscriptions for Google Cloud Pub/Sub.

```bash
watermill-cli googlecloud subscription add -t <topic> <subscription_id>

watermill-cli googlecloud subscription rm <subscription_id>
```

Additional flags are available for `subscription add` to regulate the newly created subscription's settings.
