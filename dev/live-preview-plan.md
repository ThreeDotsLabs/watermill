# Watermill Inspector — plan

Status: proof of concept in progress (branch `live-preview`).

## Goal

Expose an HTTP handler from Watermill that shows the current setup as an
interactive graph: routers, handlers, publishers, subscribers, CQRS, FanIn,
FanOut, Forwarder, topics and, where the Pub/Sub supports it, the physical
resources behind them (brokers, exchanges, queues, tables, consumer groups).
Messages moving through the pipeline appear live and can be inspected.

Hard requirements:

- Optional. Watermill behaves exactly as before when it is not enabled.
- Easy to enable in an existing system: update Watermill, add one line, open a
  route.
- Nodes can be moved around on a whiteboard-like surface. Positions are not
  saved.

## What the code allows today

- The router already knows most of the graph. `Router.AddHandler` records the
  handler name, subscribe topic, subscriber, publish topic and publisher, and
  `handler.handleMessage` is one choke point for received / error / produced /
  ack / nack.
- Core components are routers underneath: CQRS processors, Forwarder, FanIn,
  FanOut and Requeuer all end in `AddHandler` / `AddConsumerHandler`. They only
  need to label what they add.
- Forwarder, FanIn and FanOut create their own internal router, so a per-router
  switch (like `metrics.AddPrometheusRouterMetrics`) would miss them. The hook
  has to be process-wide.
- The message context already carries the handler name, and
  `EventBus.Publish(ctx, …)` sets that context on the outgoing message, so a
  publish made inside a handler can be linked back to the handler.
- Plugin Pub/Subs have no shared seam: every constructor has a different
  signature. Physical names live in configs, often as functions of the topic:

  | Plugin       | Where the physical names live                                      |
  |--------------|--------------------------------------------------------------------|
  | Kafka        | `Brokers`, `ConsumerGroup`                                         |
  | SQL          | schema and offsets adapters (table per topic), `ConsumerGroup`     |
  | AMQP         | `Exchange.GenerateName`, `Queue.GenerateName`, `GenerateRoutingKey` |
  | Google Cloud | `ProjectID`, `GenerateSubscriptionName`                            |
  | Redis        | `ConsumerGroup`, `Consumer`                                        |
  | NATS         | subject and durable calculators                                    |
  | SNS/SQS      | `TopicResolver`, `QueueUrlResolver`                                |

## Design

### 1. `topology` package (core, no dependencies)

A process-wide registry behind an atomic pointer, in the style of `expvar` or
`net/http/pprof`. When disabled, every hook is a single nil check.

- Topology: nodes (router, handler, bus, Pub/Sub, topic) and edges
  (subscribe, publish).
- Events: published, received, acked, nacked — each with message UUID, topic,
  node ID, time; payload and metadata copied (bounded) for inspection.
- It must not import `message` (the router imports it), so events carry plain
  data.

### 2. Core instruments itself

- `NewRouter`, `AddHandler` and `handleMessage` call the registry directly
  (not through decorators), so internal routers are covered.
- CQRS buses and processors, Forwarder, FanIn, FanOut, Requeuer, the delay
  publisher and the poison queue middleware add labels.
- GoChannel is the first instrumented Pub/Sub.

### 3. Plugins opt in through one optional interface

```go
type Describer interface {
    DescribeTopic(topic string) topology.Description
}
```

- The router type-asserts the handler's publisher and subscriber.
- A Pub/Sub that implements it also emits its own publish events, which makes
  publishes outside a router visible.
- Plugins that are not updated still show as a typed node (`kafka.Subscriber`)
  with Watermill topics. Publisher and subscriber of the same package are
  assumed to be the same Pub/Sub.
- Expected cost per plugin: a Watermill bump and roughly 30–50 lines.

### 4. Two views from the same data

Logical view: handlers and Watermill topics. Physical view: each topic expanded
into what `Describer` returns. A toggle in the UI, not two systems.

### 5. `inspector` package (core, separate package)

`net/http` and the embedded assets are only linked when it is imported.

```go
mux.Handle("/watermill/", http.StripPrefix("/watermill", inspector.Handler(inspector.Config{})))
```

- Standard library only: `embed` for the UI, SSE for the live stream.
- Endpoints: topology snapshot (JSON), event stream (SSE).
- UI: draggable node graph, messages animated along edges, click a message to
  see metadata, payload and its hops.

### 6. Message inspection

The server keeps a capped buffer of the most recent events (500 by default) to
replay to a browser that connects later. Writes never block; a slow client
loses events.

Payloads are opt-in (`inspector.Config{Payloads: true}`). By default they are not
copied, kept or sent: only the size is shown. When enabled, each payload is cut
to a size limit. Metadata is always included. The browser keeps the last few
hundred messages in page memory and loses them on refresh.

## Decisions

- Names: the core registry is `topology` (plugins implement `topology.Describer`, like Kafka Streams'
  `TopologyDescription`), and the UI is `components/inspector`. It was `introspect` and `preview` at first:
  "introspection" reads as reflection in Go, and "preview" reads as a pre-release.
- Global registry: yes. It is what makes "flip a switch" possible and covers
  internal routers.
- Location: in the core repo for now, as its own package. Can move to a
  separate `go.mod` later if the module weight becomes a problem.
- Switch: one line of code. No env var for now (it would force core to always
  link `net/http`).
- Order of work: proof of concept first, design doc after, based on what
  worked.

## Accepted limits (for now)

- Single process. Other services appear only as topics with no visible
  producer or consumer. Merging several services is a later step.
- The inspector has to be enabled before components are created; components
  created earlier are missed.
- Topics chosen at publish time (CQRS `GeneratePublishTopic`, raw `Publish`)
  only appear once a message flows.
- User-written publisher/subscriber decorators show as opaque nodes.
- No auth. The user mounts the handler; it is a development tool.

## Proof of concept scope

Example: `_examples/basic/7-inspector`, a single process on GoChannel (no
Docker), with:

- a router with plain handlers chained across topics
- CQRS command bus, event bus and processors
- a FanIn merging two topics
- a FanOut feeding two handlers
- a handler that fails randomly, with retry and poison queue middleware
- a background loop publishing messages

Feature, limited to what the example needs:

- `topology` registry and router instrumentation
- labels from CQRS, FanIn, FanOut and the poison queue
- GoChannel implementing `Describer`
- `inspector` handler: topology snapshot, SSE stream, bounded event buffer
- UI: single embedded HTML/JS file, no build step, no dependencies

Not in the proof of concept: real plugins, the physical-view toggle, redaction,
new tests, docs.

## Phases after the proof of concept

1. Harden `topology` and the core instrumentation; tests; benchmarks of the
   disabled path.
2. UI: physical view, better layout.
3. `Describer` in Kafka, SQL and AMQP.
4. Remaining plugins; multi-service merge.

## Proof of concept: what was built and what it showed

Built:

- `topology/` — the registry (nodes, edges, events, `Describer`, message source in context).
- Router, CQRS buses and processors, FanIn, FanOut, Forwarder, Requeuer and poison queue instrumentation.
- GoChannel and FanOut implement `Describer`; GoChannel reports its own publishes.
- `components/inspector/` — HTTP handler (topology JSON, SSE stream, replay buffer) and the embedded UI.
- `_examples/basic/7-inspector` — run with `docker compose up -d` and `go run .`, open http://localhost:8787.
  Commands and webhooks go through GoChannel, events and the poison queue through Kafka.
  `OrderPlaced` goes through a PostgreSQL outbox and the Forwarder.
- SQL `Describer` in `../watermill-sql` (branch `live-preview`): topics report their table, subscribers
  report their consumer group and offsets table, and the publisher reports its publishes.
- Kafka `Describer` in `../watermill-kafka` (branch `live-preview`): the Pub/Sub is identified by its
  brokers, topics report the Kafka topic and brokers, subscribers report their consumer group,
  and the publisher reports its publishes. About 50 lines plus a temporary `replace` to the local watermill.
- Google Cloud `Describer` in `../watermill-googlecloud` (branch `live-preview`): the Pub/Sub is the project
  (project IDs are globally unique, so no lookup is needed), topics report the fully qualified topic,
  subscribers report their subscription (the topic may be in `TopicProjectID`, the subscription in `ProjectID`),
  and the publisher reports its publishes. With the emulator, project IDs are arbitrary, so all emulators
  with the same project ID are shown as one. Not tried in the example yet.

Findings:

- A shared Pub/Sub needs an ID that every service computes the same way. Config values don't work: services
  reach the same Kafka under different addresses, and every SQL database looks the same from its dialect.
  Kafka is identified by the cluster ID from a metadata request. It's read in the background, so nothing waits
  for the broker: until it arrives (or if it never does), the broker list is the ID, and then
  `Registry.ReplacePubSub` moves the Pub/Sub, its topics, edges and stats to the cluster ID. Events with the old
  topic IDs (handlers keep the IDs they got when added) are translated by the registry. SQL takes an optional `DatabaseID`, or asks the database once (PostgreSQL `system_identifier` and
  `current_database()`, MySQL `@@server_uuid` and `DATABASE()`). A publisher on a transaction can't ask (an
  error would abort the transaction), so it uses the database found by others in the process if there is
  exactly one. A database that can't be identified is local to its service rather than merged with others.
- A consumer group belongs to a subscription, not to a topic: one topic has many. `Description` got a
  separate `SubscriptionResources` field, and the registry keeps these resources on the subscribe edge.
  Queues (AMQP, SQS) and subscriptions (Google Cloud) will fit the same place.
- Message UUID survives Kafka, so a message is followed across the broker. The link to the handler that
  published it is kept, because the publisher reports the publish before the context is lost.

- Publishes made inside a handler through a bus or a raw publisher are linked to the handler through the
  message context. These edges are not declared anywhere, so they appear after the first message (dashed in the UI).
- A handler behind the poison queue middleware never nacks: the router sees an ack and a publish to the
  poison topic. Retries are not visible either. Middlewares need their own events to show this.
- Publishers are often wrapped (the Forwarder's publisher wraps the SQL one). To report a publish once,
  the outer layer (router or bus) puts a tracker in the message context, and reports the publish itself
  only if nothing below did. This replaced the first idea that a `Describer` must report its publishes.
  A handler whose static publisher is a decorator still gets a wrong declared topic node.
- A SQL publisher or subscriber gets a database handle, often a transaction, so it doesn't know which
  database it uses. All of one dialect are shown as one Pub/Sub ("PostgreSQL"). Needs a user-given name.
- With a transaction as the handle, the SQL publisher reports the publish before the commit.
- The Forwarder's envelope has its own UUID, so the message changes UUID twice on the way through the
  outbox. The hops are connected by the "caused by" link, not by one UUID.
- The registry doesn't keep publishers or subscribers passed to `Topic`, so publishers created per
  transaction don't leak. `DescribeTopic` is called for every publish and has to be cheap.
- The registry keeps references to routers, handlers and Pub/Subs, so they are not garbage collected
  while the topology registry is enabled.
- Payload and metadata are copied for every received and published message while enabled.
  The disabled path is a nil check and a string comparison per message; it is not benchmarked yet.
- The example needs a `replace` directive until the feature is released, so it has no
  `.validate_example.yml` yet.

## Multiple services, one inspector (built in the proof of concept)

### Shape

- Symmetric, no master. Every service runs the same inspector handler. The one opened in a browser pulls
  the other services and merges them for that session. A standalone hub is the same handler in a process
  with no Watermill components of its own.
- Each service exposes its local stream (its own topology and events). An inspector always asks peers for
  the local stream, never the merged one, so two services can't feed each other's data back.
- Connections to peers are opened when the first browser connects and closed when the last one leaves.
  The connection is kept open and data is sent as it happens; nothing is polled, apart from reconnecting.
- A service with no discovery configured behaves as a single-service inspector.

### Merging

- Routers, handlers, buses and GoChannel get a service prefix in their IDs.
- Shared Pub/Subs keep a global ID, so their topics merge and connect the services.
  `Description` needs a "shared or local" flag.
- Messages are stitched by UUID. Events are ordered by arrival, because clocks of services differ.
- Replicas of one service are collapsed into one set of nodes; events are tagged with the instance.
- Still needed: a service name (config, defaults to the binary name), a way to name or alias a Pub/Sub
  (Kafka seen under different addresses, SQL databases), a protocol version.

### Discovery

A generic interface, with the Pub/Sub implementation first:

```go
type Discovery interface {
    Announce(ctx context.Context, self Peer) error
    Peers(ctx context.Context) (<-chan []Peer, error)
}
```

- Pub/Sub discovery: each service publishes a small heartbeat (service name, instance, inspector URL)
  on a well-known topic, using a publisher and a subscriber it already has. Only discovery goes through
  the broker; topology and events go over HTTP.
- Every instance must receive every announcement, so the inspector takes a subscriber constructor
  (with Kafka, each instance needs its own consumer group or none).
- The inspector must not observe its own discovery topic.
- A service has to know the URL others can reach it on: a config value, or a guess from the hostname
  and the listen port.
- Other implementations possible later: a static list, DNS names that expand to all addresses.

### Security

- A shared token is required whenever discovery is configured. The handler refuses to start without it.
- Announcements are signed: name, instance, URL, a timestamp and an HMAC made with the token.
  Unsigned, wrongly signed or old announcements are dropped. The token never goes through the broker.
  Without signing, anyone able to publish to the discovery topic could announce their own URL and
  receive the token from every inspector.
- The local stream endpoint accepts only requests that prove knowledge of the token.
- Requests to peers: http or https only, no redirects, timeouts, a limit on what is read.
- Payloads stay a per-service choice: a service with payloads off never sends them.
- A cap on concurrent stream clients. No CORS headers.
- No built-in browser authentication and no `Authorize` hook: the user wraps the handler.
- Not covered: transport (use https or a mesh), and it stays a development and staging tool.

### What was built and what it showed

- `components/inspector`: `NewHandler(Config)` replaces `Handler(Config)`. `Config` got `ServiceName`,
  `Discovery`, `Token`, `AdvertiseURL` and `Logger`. `discovery.go` has the `Discovery` interface,
  `PubSubDiscovery`, signing and the list of live instances. `merge.go` reads the streams of other
  instances and merges topologies and events.
- Every browser connection is its own session: it opens its own connections to the other instances and
  closes them when the browser goes away. There is no shared merged state and no reference counting.
  The browser drops what it has when a new session starts.
- `topology`: nodes have a `Shared` flag (set from `PubSub.Shared`), and `IgnoreTopic` keeps the
  discovery topic out of the inspector.
- The example is five services (`orders`, `payments`, `shipping`, `notifications`, `webhooks`) that
  share Kafka. All of them run in Docker Compose, built from the local copies of the three repositories.

Findings:

- `AdvertiseURL` has to be given. An `http.Handler` doesn't know its port or the path it is mounted
  under, so it can't be guessed. A helper that also starts the HTTP server could guess it.
- Kafka's `SubscribeInitialize` needs extra configuration, so discovery announces first (the publish
  creates the topic) and then subscribes, with a few retries.
- Replicas collapse for free: instances of one service have the same node IDs, because routers and
  buses are numbered in the order they are created. This breaks if the order is not deterministic.
- An instance that stops disappears from the service list after about 12 seconds, but the UI doesn't
  remove nodes that are gone.
- Events of other services arrive later than they happened (each connection starts with a replay),
  so the path of a message is sorted by the time of the event. Clocks that differ between hosts
  will show hops in a wrong order.
- Not done: aliases for a Pub/Sub seen under different addresses, a name for SQL databases,
  the limit on concurrent streams is a fixed number, and there are no tests.

### Service frames in the UI

- Every service is a frame around its nodes, laid out as a horizontal lane. The flow inside a lane is
  still left to right, and columns are shared by all lanes.
- Topics of shared Pub/Subs are never inside a frame, even when only one service uses them (the outbox
  table, the poison queue): a topic doesn't belong to a service. They are in a lane of their own,
  between the services that use them. Services that use no shared topics are below.
  Topics of a local Pub/Sub (GoChannel) are inside the frame of their service.
- The frame has the name of the service and the number of instances. It replaced the legend and the
  color bar on handlers. Dragging a frame moves its nodes; a frame stretches around a dragged node.
- Nodes and frames of a service that is gone are removed.

## Rates on the nodes

Nodes show rates instead of total counts, which only made sense in an example.

- A handler shows messages handled per second and a dot: grey when idle, green when working, red when any message failed.
  While it fails, its caption is replaced with the rate of failures.
- A topic shows messages published per second.
- The detail of a handler also has the average handling time.
- The registry counts totals per node (published, handled, failed, handling time) for every event, with atomic counters.
  `Registry.Stats()` returns them. They are right also when a slow subscriber loses events.
- The inspector sends the totals every second as a `stats` event. With many services, totals of nodes that become
  one node (a handler of two instances, a topic that two services publish to) are added up.
- The browser calculates rates from the totals, as an average over the last 10 seconds, the way Prometheus does with counters.
  When a total goes down (an instance is gone), the rate starts from scratch.
- A message that goes to the poison queue is acked, so its handler stays green. Only the rate of the poison topic shows it.

## Components with no connections

A component with no edges, like a bus that has not published anything yet, is not drawn. It appears with its first message.
A bus used only inside handlers never appears: these publishes are shown as edges from the handler.

## Failures dealt with by middlewares

A handler is red when it has failed any message in the last 10 seconds.
The poison queue middleware turns an error into an ack, so the router would never see the failure.
The middleware calls `topology.ReportFailure` with the context of the message, and the router reports the message
as failed although it's acked. Other middlewares that swallow errors can do the same.
Retries are not visible: a message that works on the second attempt is a success.
(This replaces the earlier note that a handler with a poison queue stays green.)
