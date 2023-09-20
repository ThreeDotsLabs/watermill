+++
title = "CQRS Component"
description = "Build CQRS and Event-Driven applications"
date = 2019-02-12T12:47:30+01:00
weight = -400
draft = false
bref = "Golang CQRS implementation in Watermill"
toc = true
+++

## CQRS

> CQRS means "Command-query responsibility segregation". We segregate the responsibility between commands (write requests) and queries (read requests). The write requests and the read requests are handled by different objects.
>
> That's it. We can further split up the data storage, having separate read and write stores. Once that happens, there may be many read stores, optimized for handling different types of queries or spanning many bounded contexts. Though separate read/write stores are often discussed in relation with CQRS, this is not CQRS itself. CQRS is just the first split of commands and queries.
>
> Source: [www.cqrs.nu FAQ](http://www.cqrs.nu/Faq/command-query-responsibility-segregation)

![CQRS Schema](https://threedots.tech/watermill-io/cqrs-big-picture.svg)

The `cqrs` component provides some useful abstractions built on top of Pub/Sub and Router that help to implement the CQRS pattern.

You don't need to implement the entire CQRS. It's very common to use just the event part of this component to build event-driven applications.

### Building blocks

#### Event

The event represents something that already took place. Events are immutable.

#### Event Bus

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_bus.go" first_line_contains="// EventBus" last_line_contains="type EventBus" padding_after="0" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_bus.go" first_line_contains="type EventBusConfig" last_line_contains="func (c *EventBusConfig) setDefaults()" padding_after="4" %}}
{{% /render-md %}}

#### Event Processor

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_processor.go" first_line_contains="// EventProcessor" last_line_contains="type EventProcessor" padding_after="0" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_processor.go" first_line_contains="type EventProcessorConfig" last_line_contains="func (c *EventProcessorConfig) setDefaults()" padding_after="4" %}}
{{% /render-md %}}

#### Event Group Processor

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_processor_group.go" first_line_contains="// EventGroupProcessor" last_line_contains="type EventGroupProcessor" padding_after="0" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_processor_group.go" first_line_contains="type EventGroupProcessorConfig" last_line_contains="func (c *EventGroupProcessorConfig) setDefaults()" padding_after="4" %}}
{{% /render-md %}}

Learn more in [Event Group Processor](#event-handler-groups).

#### Event Handler

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_handler.go" first_line_contains="// EventHandler" last_line_contains="type EventHandler" padding_after="0" %}}
{{% /render-md %}}

#### Command

The command is a simple data structure, representing the request for executing some operation.

#### Command Bus

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/command_bus.go" first_line_contains="// CommandBus" last_line_contains="type CommandBus" padding_after="0" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/command_bus.go" first_line_contains="type CommandBusConfig" last_line_contains="func (c *CommandBusConfig) setDefaults()" padding_after="4" %}}
{{% /render-md %}}

#### Command Processor

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/command_processor.go" first_line_contains="// CommandProcessor" last_line_contains="type CommandProcessor" padding_after="0" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/command_processor.go" first_line_contains="type CommandProcessorConfig" last_line_contains="func (c *CommandProcessorConfig) setDefaults()" padding_after="4" %}}
{{% /render-md %}}

#### Command Handler

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/command_handler.go" first_line_contains="// CommandHandler" last_line_contains="type CommandHandler" padding_after="0" %}}
{{% /render-md %}}

#### Command and Event Marshaler

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/marshaler.go" first_line_contains="// CommandEventMarshaler" last_line_contains="NameFromMessage(" padding_after="1" %}}
{{% /render-md %}}

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

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="bookRoomCmd := &BookRoom{" last_line_contains="panic(err)" padding_after="1" %}}
{{% /render-md %}}

### Command handler

`BookRoomHandler` will handle our command.

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="// BookRoomHandler is a command handler" last_line_contains="// OrderBeerOnRoomBooked is a event handler" padding_after="0" %}}
{{% /render-md %}}

### Event handler

As mentioned before, we want to order a beer every time when a room is booked (*"Whenever a Room is booked"* post-it). We do it by using the `OrderBeer` command.

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="// OrderBeerOnRoomBooked is a event handler" last_line_contains="// OrderBeerHandler is a command handler" padding_after="0" %}}
{{% /render-md %}}

`OrderBeerHandler` is very similar to `BookRoomHandler`. The only difference is, that it sometimes returns an error when there are not enough beers, which causes redelivery of the command.
You can find the entire implementation in the [example source code](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/basic/5-cqrs-protobuf/?utm_source=cqrs_doc).

### Event Handler groups

By default, each event handler has a separate subscriber instance.
It works fine, if just one event type is sent to the topic.

In the scenario, when we have multiple event types on one topic, you have two options:

1. You can set `EventConfig.AckOnUnknownEvent` to true - it will acknowledge all events that are not handled by handler,
2. You can use Event Handler groups mechanism.

To use event groups, you need to set `GenerateHandlerGroupSubscribeTopic` and `GroupSubscriberConstructor` options in [`EventConfig`](#event-config).

After that, you can use `AddHandlersGroup` on [`EventProcessor`](#event-processor).

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="eventProcessor.AddHandlersGroup(" last_line_contains="if err != nil {" padding_after="0" %}}
{{% /render-md %}}

Both `GenerateHandlerGroupSubscribeTopic` and `GroupSubscriberConstructor` receives information about group name in function arguments.

### Generic handlers

Since Watermill v1.3 it's possible to use generic handlers for commands and events. It's useful when you have a lot of commands/events and you don't want to create a handler for each of them.

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="cqrs.NewGroupEventHandler" last_line_contains="})," padding_after="0" %}}
{{% /render-md %}}

Under the hood, it creates EventHandler or CommandHandler implementation.
It's available for all kind of handlers.

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/command_handler.go" first_line_contains="// NewCommandHandler" last_line_contains="func NewCommandHandler" padding_after="0" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_handler.go" first_line_contains="// NewEventHandler" last_line_contains="func NewEventHandler" padding_after="0" %}}
{{% /render-md %}}

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_handler.go" first_line_contains="// NewGroupEventHandler" last_line_contains="func NewGroupEventHandler" padding_after="0" %}}
{{% /render-md %}}

### Building a read model with the event handler

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="// BookingsFinancialReport is a read model" last_line_contains="func main() {" padding_after="0" %}}
{{% /render-md %}}

### Wiring it up

We have all the blocks to build our CQRS application.

We will use the AMQP (RabbitMQ) as our message broker: [AMQP]({{< ref "/pubsubs/amqp" >}}).

Under the hood, CQRS is using Watermill's message router. If you are not familiar with it and want to learn how it works, you should check [Getting Started guide]({{< ref "getting-started" >}}).
It will also show you how to use some standard messaging patterns, like metrics, poison queue, throttling, correlation and other tools used by every message-driven application. Those come built-in with Watermill.

Let's go back to the CQRS. As you already know, CQRS is built from multiple components, like Command or Event buses, handlers, processors, etc.

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="main() {" last_line_contains="err := router.Run(" padding_after="3" %}}
{{% /render-md %}}

And that's all. We have a working CQRS application.

### What's next?

As mentioned before, if you are not familiar with Watermill, we highly recommend reading [Getting Started guide]({{< ref "getting-started" >}}).
