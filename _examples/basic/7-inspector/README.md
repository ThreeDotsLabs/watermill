# Inspector

This example runs five small services and serves the Watermill Inspector for all of them: routers, handlers,
CQRS buses, the Forwarder, FanIn, FanOut and topics as a graph, with messages flowing through it.

The inspector is a proof of concept. See `dev/live-preview-plan.md` in the repository root.

## Running

The example uses local copies of `watermill`, `watermill-kafka` and `watermill-sql` (see `replace` in `go.mod`),
so the `watermill-kafka` and `watermill-sql` repositories have to be cloned next to `watermill`,
on the `live-preview` branch.

```bash
docker compose up -d
```

It starts Kafka, PostgreSQL and the five services. payments and shipping run two instances each.
The first start takes a while, because the services are compiled. Then open the inspector of any instance.
Each one shows all running services.

| Service       | Instance      | Inspector             |
|---------------|---------------|-----------------------|
| orders        | orders        | http://localhost:8787 |
| payments      | payments-1    | http://localhost:8788 |
| payments      | payments-2    | http://localhost:8792 |
| webhooks      | webhooks      | http://localhost:8789 |
| shipping      | shipping-1    | http://localhost:8790 |
| shipping      | shipping-2    | http://localhost:8793 |
| notifications | notifications | http://localhost:8791 |

- Drag nodes to move them, drag a frame to move a service, drag the background to pan, scroll to zoom.
- Click a message on the right to see its payload, metadata and path.
- Click a node to see its details and recent messages.

After changing the code, restart a service to compile and run it again:

```bash
docker compose restart orders
docker compose logs -f orders
```

Stopping a service (`docker compose stop notifications`) removes it from the inspector after a few seconds.
Stopping one instance (`docker compose stop shipping-2`) only changes the number of instances shown next to the service.

A service can also be run on the host with `go run . -service orders`, with Kafka and PostgreSQL from
Docker Compose. Don't mix it with services running in Docker Compose: they know Kafka under a different
address, so the inspector would show two separate Kafkas.

## What's inside

- **orders**: the `PlaceOrder` command is sent with the command bus through GoChannel. Its handler publishes
  `OrderPlaced` with the outbox pattern: the event is stored in a PostgreSQL table first, and the Forwarder
  moves it to Kafka. Two event handlers mark the order as paid and as shipped.
- **payments**: the `ChargePayment` handler reads `OrderPlaced` from Kafka and fails often.
  It is retried and then the message goes to a poison queue. On success, it publishes `PaymentCharged`.
  The `reconcile-payment` handler reads payment updates published by the webhooks service.
- **shipping**: `PaymentCharged` schedules packing with a `PackOrder` command sent through GoChannel.
  The command handler publishes `OrderShipped` to Kafka.
- **notifications**: sends e-mails about placed and shipped orders. It only reads events from Kafka.
- **webhooks**: webhooks published to two GoChannel topics are merged by a FanIn, normalized by a plain
  router handler, and delivered to two handlers through a FanOut. One of them publishes the update
  to Kafka, where the payments service reads it.

Each event handler has its own Kafka consumer group. Instances of one service share the consumer groups,
so they split the messages between them. Kafka creates topics with four partitions (see `kafka-setup`
in `docker-compose.yml`), so each instance gets some of them.

Instances of one service have the same `ServiceName`, so the inspector shows them as one service.

## How the services find each other

Every service announces the address of its inspector on a Kafka topic (`watermill_inspector_discovery`).
The inspector opened in the browser connects to the others and merges what they send.
The services share a token (`inspectorToken` in `main.go`): announcements are signed with it,
and an inspector gives its data only to a request with the token.
