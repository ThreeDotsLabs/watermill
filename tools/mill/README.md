# mill - a simple CLI tool for Watermill

`mill` is a CLI tool for the [Watermill](https://watermill.io) library.

It has two basic functionalities, namely producing and consuming messages on the following
Pub/Subs:

1. Kafka
2. Google Cloud Pub/Sub
3. RabbitMQ

See Watermill's [Supported Pub/Subs](https://watermill.io/pubsubs) for more details on how this works.

## Installation

To install this tool, just execute:

```bash
go get -u github.com/ThreeDotsLabs/watermill/tools/mill
```

This will install a `mill` binary in your system.

## Consume mode

In consume mode, the tool subscribes to a topic/queue/subscription (nomenclature depending on the particular Pub/Sub provider)
and prints the messages' payload to the standard output.

Other outputs, for example ones that add a timestamp or preserve UUIDs or metadata, are easily attainable by modification
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
mill <provider> <command>
```

with the appropriate flags regulating the specific behaviour of each command.

`command` is usually one of `produce` or `consume`, but some providers may handle additional commands
that are specific for them.

The flags are context-specific, so the best way to find out about them is to use the `-h` flag and study 
which flags are available/required for the specific context and act accordingly.

### Advanced usage

A neat feature of producers and consumers is that you can use the power of stdin/stdout piping for stuff like:

```bash
myservice | tee myservice.log | mill kafka produce -t myservice-logs --brokers kafka-host:8082 
```

And on another host:

```bash
mill kafka consume -t myservice-logs --brokers kafka-host:8082 >> myservice.log
```

In the above example, the host on which `myservice` runs has its own copy of `myservice.log` and any host that consumes
from the kafka topic will replicate the log entries in their local copy.


## Additional functionalities

### Google Cloud Pub/Sub

#### Adding/Removing subscriptions

You can use `mill` to create/remove subscriptions for Google Cloud Pub/Sub:

```bash
mill googlecloud subscription add -t <topic> <subscription_id>

mill googlecloud subscription rm <subscription_id>
```

Additional flags are available for `subscription add` to regulate the newly created subscription's settings.

#### Listing subscriptions

You can use `mill` to list existings subscriptions:

```bash
mill googlecloud subscription ls [-t topic]
```

The topic is optional. If omitted, all topics will be listed with their subscriptions.
