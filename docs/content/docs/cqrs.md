+++
title = "CQRS Component"
description = "Build CQRS and Event-Driven applications"
date = 2019-02-12T12:47:30+01:00
weight = -400
draft = false
bref = "Go CQRS implementation in Watermill"
+++

## CQRS

> CQRS means "Command-query responsibility segregation". We segregate the responsibility between commands (write requests) and queries (read requests). The write requests and the read requests are handled by different objects.
>
> That's it. We can further split up the data storage, having separate read and write stores. Once that happens, there may be many read stores, optimized for handling different types of queries or spanning many bounded contexts. Though separate read/write stores are often discussed in relation with CQRS, this is not CQRS itself. CQRS is just the first split of commands and queries.
>
> Source: [www.cqrs.nu FAQ](http://www.cqrs.nu/Faq/command-query-responsibility-segregation)

<img src="https://threedots.tech/watermill-io/cqrs-big-picture.svg" alt="CQRS Schema" style="width:100%; margin-bottom: 3rem; background-color: white; padding: 2rem;" />

The `cqrs` component provides some useful abstractions built on top of Pub/Sub and Router that help to implement the CQRS pattern.

You don't need to implement the entire CQRS. It's very common to use just the event part of this component to build event-driven applications.

### Building blocks

#### Event

The event represents something that already took place. Events are immutable.

#### Event Bus

{{% load-snippet-partial file="src-link/components/cqrs/event_bus.go" first_line_contains="// EventBus" last_line_contains="type EventBus" padding_after="0" %}}

{{% load-snippet-partial file="src-link/components/cqrs/event_bus.go" first_line_contains="type EventBusConfig" last_line_contains="func (c *EventBusConfig) setDefaults()" padding_after="4" %}}

#### Event Processor

{{% load-snippet-partial file="src-link/components/cqrs/event_processor.go" first_line_contains="// EventProcessor" last_line_contains="type EventProcessor" padding_after="0" %}}

{{% load-snippet-partial file="src-link/components/cqrs/event_processor.go" first_line_contains="type EventProcessorConfig" last_line_contains="func (c *EventProcessorConfig) setDefaults()" padding_after="4" %}}

#### Event Group Processor

{{% load-snippet-partial file="src-link/components/cqrs/event_processor_group.go" first_line_contains="// EventGroupProcessor" last_line_contains="type EventGroupProcessor" padding_after="0" %}}

{{% load-snippet-partial file="src-link/components/cqrs/event_processor_group.go" first_line_contains="type EventGroupProcessorConfig" last_line_contains="func (c *EventGroupProcessorConfig) setDefaults()" padding_after="4" %}}

Learn more in [Event Group Processor](#event-handler-groups).

#### Event Handler

{{% load-snippet-partial file="src-link/components/cqrs/event_handler.go" first_line_contains="// EventHandler" last_line_contains="type EventHandler" padding_after="0" %}}

#### Command

The command is a simple data structure, representing the request for executing some operation.

#### Command Bus

{{% load-snippet-partial file="src-link/components/cqrs/command_bus.go" first_line_contains="// CommandBus" last_line_contains="type CommandBus" padding_after="0" %}}

{{% load-snippet-partial file="src-link/components/cqrs/command_bus.go" first_line_contains="type CommandBusConfig" last_line_contains="func (c *CommandBusConfig) setDefaults()" padding_after="4" %}}

#### Command Processor

{{% load-snippet-partial file="src-link/components/cqrs/command_processor.go" first_line_contains="// CommandProcessor" last_line_contains="type CommandProcessor" padding_after="0" %}}

{{% load-snippet-partial file="src-link/components/cqrs/command_processor.go" first_line_contains="type CommandProcessorConfig" last_line_contains="func (c *CommandProcessorConfig) setDefaults()" padding_after="4" %}}

#### Command Handler

{{% load-snippet-partial file="src-link/components/cqrs/command_handler.go" first_line_contains="// CommandHandler" last_line_contains="type CommandHandler" padding_after="0" %}}

#### Command and Event Marshaler

{{% load-snippet-partial file="src-link/components/cqrs/marshaler.go" first_line_contains="// CommandEventMarshaler" last_line_contains="NameFromMessage(" padding_after="1" %}}

#### Command and Event Marshaler Decorator

Sometimes it's useful to add extra metadata to each command or event after marshaling it to a message. For example, you may want to add a partition key to each message using Kafka.

You can use `CommandEventMarshalerDecorator` to extend a marshaler with an extra step.

{{% load-snippet-partial file="src-link/components/cqrs/marshaler.go" first_line_contains="// CommandEventMarshalerDecorator" last_line_contains="}" padding_after="0" %}}

```go
type Event interface {
	PartitionKey() string
}

// ...

cqrsMarshaler := CommandEventMarshalerDecorator{
	CommandEventMarshaler: cqrs.JSONMarshaler{},
	DecorateFunc: func(v any, msg *message.Message) error {
		pm, ok := v.(Event)
		if !ok {
			return fmt.Errorf("%T does not implement Event and can't be marshaled", v)
		}

		partitionKey := pm.PartitionKey()
		if partitionKey == "" {
			return fmt.Errorf("PartitionKey is empty")
		}

		msg.Metadata.Set(PartitionKeyMetadataField, partitionKey)
		return nil
	},
}
```

## Usage

### Example domain

As an example, we will use a simple domain, that is responsible for handing room booking in a hotel.

We will use **Event Storming** notation to show the model of this domain.

Legend:

- **blue** post-its are commands
- **orange** post-its are events
- **green** post-its are read models, asynchronously generated from events
- **violet** post-its are policies, which are triggered by events and produce commands
- **pink** post its are hot-spots; we mark places where problems often occur

![CQRS Event Storming](https://threedots.tech/watermill-io/cqrs-example-storming.png)

The domain is simple:

- A Guest is able to **book a room**.
- **Whenever a room is booked, we order a beer** for the guest (because we love our guests).
    - We know that sometimes there are **not enough beers**.
- We generate a **financial report** based on the bookings.


### Sending a command

For the beginning, we need to simulate the guest's action.

{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="bookRoomCmd := &BookRoom{" last_line_contains="panic(err)" padding_after="1" %}}

### Command handler

`BookRoomHandler` will handle our command.

{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="// BookRoomHandler is a command handler" last_line_contains="// OrderBeerOnRoomBooked is an event handler" padding_after="0" %}}

### Event handler

As mentioned before, we want to order a beer every time when a room is booked (*"Whenever a Room is booked"* post-it). We do it by using the `OrderBeer` command.

{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="// OrderBeerOnRoomBooked is an event handler" last_line_contains="// OrderBeerHandler is a command handler" padding_after="0" %}}

`OrderBeerHandler` is very similar to `BookRoomHandler`. The only difference is, that it sometimes returns an error when there are not enough beers, which causes redelivery of the command.
You can find the entire implementation in the [example source code](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/basic/5-cqrs-protobuf/?utm_source=cqrs_doc).

### Event Handler groups

By default, each event handler has a separate subscriber instance.
It works fine, if just one event type is sent to the topic.

In the scenario, when we have multiple event types on one topic, you have two options:

1. You can set `EventConfig.AckOnUnknownEvent` to true - it will acknowledge all events that are not handled by handler,
2. You can use Event Handler groups mechanism.

**Key differences between `EventProcessor` and `EventGroupProcessor`:**

1. `EventProcessor`:
- Each handler has its own subscriber instance
- One handler per event type
- Simple one-to-one matching of events to handlers

2. `EventGroupProcessor`:
- Group of handlers share a single subscriber instance (and one consumer group, if such mechanism is supported -- allows to maintain order of events),
- One handler group can support multiple event types,
- When message arrives to the topic, Watermill will match it to the handler in the group based on event type

```kroki {type=mermaid}
graph TD
    subgraph Individual Handlers
        E[Event Bus] --> S1[Subscriber 1]
        E --> S2[Subscriber 2]
        E --> S3[Subscriber 3]
        S1 --> H1[Handler 1]
        S2 --> H2[Handler 2]
        S3 --> H3[Handler 3]
    end

    subgraph Group Handlers
        EB[Event Bus] --> SharedSub[Shared Subscriber]
        SharedSub --> GH[Handler Group]
        GH --> GH1[Handler 1]
        GH --> GH2[Handler 2]
        GH --> GH3[Handler 3]
    end
```

**Event Handler groups are helpful when you have multiple event types on one topic and you want to maintain order of events.**
Thanks to using one subscriber instance and consumer group, events will be processed in the order they were sent.

{{< callout context="note" title="Note" icon="outline/info-circle" >}}
It's supported to have multiple handlers for the same event type in one group, but we recommend to not do that.

Please keep in mind that those handlers will be processed within the same message.
If first handler succeeds and the second fails, the message will be re-delivered and the first will be re-executed.
{{< /callout >}}

To use event groups, you need to set `GenerateHandlerGroupSubscribeTopic` and `GroupSubscriberConstructor` options in [`EventConfig`](#event-config).

After that, you can use `AddHandlersGroup` on [`EventProcessor`](#event-processor).

{{% load-snippet-partial file="src-link/_examples/basic/6-cqrs-ordered-events/main.go" first_line_contains="eventProcessor.AddHandlersGroup(" last_line_contains="if err != nil {" padding_after="0" %}}

Both `GenerateHandlerGroupSubscribeTopic` and `GroupSubscriberConstructor` receives information about group name in function arguments.

You can see a fully working example with event groups in our [examples](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/basic/6-cqrs-ordered-events/).

### Generic handlers

Since Watermill v1.3 it's possible to use generic handlers for commands and events. It's useful when you have a lot of commands/events and you don't want to create a handler for each of them.

{{% load-snippet-partial file="src-link/_examples/basic/6-cqrs-ordered-events/main.go" first_line_contains="cqrs.NewGroupEventHandler" last_line_contains=")," padding_after="0" %}}

Under the hood, it creates EventHandler or CommandHandler implementation.
It's available for all kind of handlers.

{{% load-snippet-partial file="src-link/components/cqrs/command_handler.go" first_line_contains="// NewCommandHandler" last_line_contains="func NewCommandHandler" padding_after="0" %}}

{{% load-snippet-partial file="src-link/components/cqrs/event_handler.go" first_line_contains="// NewEventHandler" last_line_contains="func NewEventHandler" padding_after="0" %}}

{{% load-snippet-partial file="src-link/components/cqrs/event_handler.go" first_line_contains="// NewGroupEventHandler" last_line_contains="func NewGroupEventHandler" padding_after="0" %}}
### Building a read model with the event handler

{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="// BookingsFinancialReport is a read model" last_line_contains="func main() {" padding_after="0" %}}

### Wiring it up

We have all the blocks to build our CQRS application.

We will use the AMQP (RabbitMQ) as our message broker: [AMQP]({{< ref "/pubsubs/amqp" >}}).

Under the hood, CQRS is using Watermill's message router. If you are not familiar with it and want to learn how it works, you should check [Getting Started guide]({{< ref "getting-started" >}}).
It will also show you how to use some standard messaging patterns, like metrics, poison queue, throttling, correlation and other tools used by every message-driven application. Those come built-in with Watermill.

Let's go back to the CQRS. As you already know, CQRS is built from multiple components, like Command or Event buses, handlers, processors, etc.

{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="main() {" last_line_contains="err := router.Run(" padding_after="3" %}}

And that's all. We have a working CQRS application.

### What's next?

As mentioned before, if you are not familiar with Watermill, we highly recommend reading [Getting Started guide]({{< ref "getting-started" >}}).
