+++
title = "Inspector"
description = "See your Watermill setup and messages live in the browser"
weight = -350
draft = false
bref = "See your Watermill setup and messages live in the browser"
+++

The inspector is an HTTP handler that shows the running Watermill setup as a graph:
routers, handlers, CQRS buses, FanIn, FanOut, Forwarder, Pub/Subs and topics.
Messages move through the graph as they flow, and you can click one to see its metadata, payload and path.

{{< callout "danger" >}}
**The inspector is experimental. Its API will change.**

It's a development tool. It has no authentication, and it shows message metadata (and payloads, if enabled).
Don't expose it publicly.
{{< /callout >}}

## Enabling

Create the handler and mount it under any path that ends with a slash:

```go
inspectorHandler, err := inspector.NewHandler(inspector.Config{})
if err != nil {
    panic(err)
}

mux := http.NewServeMux()
mux.Handle("/watermill/", http.StripPrefix("/watermill", inspectorHandler))

go http.ListenAndServe(":8080", mux)

// create routers, Pub/Subs and other components after the inspector
router, err := message.NewRouter(message.RouterConfig{}, logger)
```

Open `http://localhost:8080/watermill/`.

**Create the inspector before any routers, Pub/Subs or other components.** Components created earlier are not visible.

If you don't create the inspector, Watermill works as before.

## Configuration

{{% load-snippet-partial file="src-link/components/inspector/inspector.go" first_line_contains="type Config struct {" last_line_contains="Logger watermill.LoggerAdapter" padding_after="1" %}}

### Payloads

Payloads are not copied by default. Only their size is shown. To see them:

```go
inspectorHandler, err := inspector.NewHandler(inspector.Config{
    Payloads: true,
})
```

Each payload is cut to `topology.MaxPayloadSize` (16 KiB).

## What you see

- **Handlers** show messages handled per second. The dot is grey when idle, green when working, and red when
  a message failed in the last 10 seconds.
- **Topics** show messages published per second.
- **Dashed edges** were not declared up front, but found from a published message (for example, an event bus used inside a handler).
- Click a node to see its details and recent messages. Click a message to see its metadata, payload and the hops it made.

Nodes can be dragged around. Positions are not saved.

A component appears with its first edge. A bus that hasn't published anything yet is not shown.

## Multiple services

Each service runs its own inspector. With `Discovery` set, they find each other, and the inspector of any service shows all of them.

```go
inspectorHandler, err := inspector.NewHandler(inspector.Config{
    ServiceName: "orders",

    Discovery:    inspector.NewPubSubDiscovery(kafkaPublisher, discoverySubscriber, ""),
    Token:        os.Getenv("INSPECTOR_TOKEN"),
    AdvertiseURL: "http://orders:8080/watermill/",
})
```

- `NewPubSubDiscovery` announces the service on a topic (`watermill_inspector_discovery` by default).
  **Every instance must receive every announcement.** With Kafka, use a subscriber without a consumer group.
- `Token` is a secret shared by all services. Announcements are signed with it, and an inspector sends its data only to requests that know it.
- `AdvertiseURL` is the URL of the handler that other services can reach.
- Instances with the same `ServiceName` are shown as one service.
- Topics of Pub/Subs shared by services (like Kafka) connect the services in the graph. GoChannel topics stay inside their service.

## Pub/Sub support

Every Pub/Sub shows up with its Watermill topics.
Pub/Subs that implement `topology.Describer` also show what's behind a topic (for example, a Kafka topic and consumer group),
report messages published outside a router, and connect topics across services.

GoChannel supports it. If you maintain a Pub/Sub, see [Implementing custom Pub/Sub](/development/pub-sub-implementing/#inspector-support).

## Limitations

- Retries are not visible. A message that succeeds on the second attempt is a success.
- Topics chosen at publish time (CQRS `GeneratePublishTopic`, a raw `Publish`) appear only after the first message.
- Your own publisher or subscriber decorators may show up as separate Pub/Subs.
- Clocks of different hosts can differ, so hops of a message across services may be shown in the wrong order.

## Example

The [inspector example](https://github.com/ThreeDotsLabs/watermill/tree/master/_examples/basic/7-inspector)
runs five services with Kafka, PostgreSQL, CQRS, the Forwarder, FanIn, FanOut and a poison queue.
