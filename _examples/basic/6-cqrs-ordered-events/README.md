# Example Golang CQRS application - ordered events with Kafka

This application is using [Watermill CQRS](http://watermill.io/docs/cqrs) component.

Detailed documentation for CQRS can be found in Watermill's docs: [http://watermill.io/docs/cqrs#usage](http://watermill.io/docs/cqrs).

This example, uses event groups to keep order for multiple events. You can read more about them in the [Watermill documentation](https://watermill.io/docs/cqrs/).
We are also using Kafka's partitioning keys to increase processing throughput without losing order of events.


## What does this application do?

This application manages an email subscription system where users can:

1. Subscribe to receive emails by providing their email address
2. Update their email address after subscribing
3. Unsubscribe from the mailing list

The system maintains:
- A current list of all active subscribers
- A timeline of all subscription-related activities

In this example, keeping order of events is crucial.
If events won't be ordered, and `SubscriberSubscribed` would arrive after `SubscriberUnsubscribed` event,
the subscriber will be still subscribed.

## Possible improvements

In this example, we are using global `events` and `commands` topics.
You can consider splitting them into smaller topics, for example, per aggregate type.

Thanks to that, you can scale your application horizontally and increase the throughput and processing less events.

## Running

```bash
docker-compose up
```
