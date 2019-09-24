+++
title = "CQRS Component"
description = "Command Query Responsibility Segregation (CQRS) Component"
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

### Glossary

![CQRS Schema](https://threedots.tech/watermill-io/cqrs-big-picture.svg)

#### Command

The command is a simple data structure, representing the request for executing some operation.

#### Command Bus

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/command_bus.go" first_line_contains="// CommandBus" last_line_contains="type CommandBus" padding_after="0" %}}
{{% /render-md %}}

#### Command Processor

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/command_processor.go" first_line_contains="// CommandProcessor" last_line_contains="type CommandProcessor" padding_after="0" %}}
{{% /render-md %}}

#### Command Handler

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/command_processor.go" first_line_contains="// CommandHandler" last_line_contains="type CommandHandler" padding_after="0" %}}
{{% /render-md %}}

#### Event

The event represents something that already took place. Events are immutable.

#### Event Bus

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_bus.go" first_line_contains="// EventBus" last_line_contains="type EventBus" padding_after="0" %}}
{{% /render-md %}}

#### Event Processor

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_processor.go" first_line_contains="// EventProcessor" last_line_contains="type EventProcessor" padding_after="0" %}}
{{% /render-md %}}

#### Event Handler

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/event_processor.go" first_line_contains="// EventHandler" last_line_contains="type EventHandler" padding_after="0" %}}
{{% /render-md %}}

#### CQRS Facade

{{% render-md %}}
{{% load-snippet-partial file="src-link/components/cqrs/cqrs.go" first_line_contains="// Facade" last_line_contains="type Facade" padding_after="0" %}}
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

### Building a read model with the event handler

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="// BookingsFinancialReport is a read model" last_line_contains="func main() {" padding_after="0" %}}
{{% /render-md %}}

### Wiring it up - the CQRS facade

We have all the blocks to build our CQRS application. We now need to use some kind of glue to wire it up.

We will use the simplest in-memory messaging infrastructure: [GoChannel]({{< ref "/pubsubs/gochannel" >}}).

Under the hood, CQRS is using Watermill's message router. If you are not familiar with it and want to learn how it works, you should check [Getting Started guide]({{< ref "getting-started" >}}).
It will also show you how to use some standard messaging patterns, like metrics, poison queue, throttling, correlation and other tools used by every message-driven application. Those come built-in with Watermill.

Let's go back to the CQRS. As you already know, CQRS is built from multiple components, like Command or Event buses, handlers, processors, etc.
To simplify creating all these building blocks, we created `cqrs.Facade`, which creates all of them.

{{% render-md %}}
{{% load-snippet-partial file="src-link/_examples/basic/5-cqrs-protobuf/main.go" first_line_contains="main() {" last_line_contains="err := router.Run(" padding_after="3" %}}
{{% /render-md %}}

And that's all. We have a working CQRS application.

### What's next?

As mentioned before, if you are not familiar with Watermill, we highly recommend reading [Getting Started guide]({{< ref "getting-started" >}}).
