# pq

pq is a CLI tool for working with delayed messages on poison queues.

For now, it supports the PostgreSQL Pub/Sub implementation.

## Install

```bash
go install github.com/ThreeDotsLabs/watermill/tools/pq@latest
```

## Usage

Set the `DATABASE_URL` environment variable to your PostgreSQL connection string.

```bash
pq -backend postgres -topic requeue
```

This will use the default `watermill_` prefix, so will use the `watermill_requeue` table.

If you use a custom prefix, use the `-raw-topic` flag instead:

```bash
pq -backend postgres -raw-topic my_prefix_requeue
```

## Commands

- Requeue — Updates the `_watermill_delayed_until` metadata to the current time, so the message will be instantly requeued.
- Ack — Removes the message from the queue (be careful — you will lose the message forever).
