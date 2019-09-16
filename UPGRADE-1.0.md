# Upgrade instructions from v0.4.X

In v1.0.0 we introduced a couple of breaking changes, to keep a stable API until version v2.

## Migrating Pub/Subs

All Pub/Subs (excluding go-channel implementation) were moved to separated repositories.
You can replace all import paths, with provided `sed`:

	find . -type f -iname '*.go' -exec sed -i -E "s/github\.com\/ThreeDotsLabs\/watermill\/message\/infrastructure\/(amqp|googlecloud|http|io|kafka|nats|sql)/github.com\/ThreeDotsLabs\/watermill-\1\/pkg\/\1/" "{}" +;
	find . -type f -iname '*.go' -exec sed -i -E "s/github\.com\/ThreeDotsLabs\/watermill\/message\/infrastructure\/gochannel/github\.com\/ThreeDotsLabs\/watermill\/pubsub\/gochannel/" "{}" +;

# Breaking changes
- `message.PubSub` interface was removed
- `message.NewPubSub` constructor was removed
- `message.NoPublishHandlerFunc` is now passed to `message.Router.AddNoPublisherHandler`, instead of `message.HandlerFunc`.
- `message.Router.Run` now requires `context.Context` in parameter
- `PrometheusMetricsBuilder.DecoratePubSub` was removed (because of `message.PubSub` interface removal)
- `cars.ObjectName` was renamed to `cqrs.FullyQualifiedStructName`
- `github.com/ThreeDotsLabs/watermill/message/infrastructure/gochannel` was moved to `github.com/ThreeDotsLabs/watermill/pubsub/gochannel`
- `middleware.Retry` configuration parameters have been renamed
- Universal Pub/Sub tests have been moved from `github.com/ThreeDotsLabs/watermill/message/infrastructure` to `github.com/ThreeDotsLabs/watermill/pubsub/tests`
- All universal tests require now `TestContext`.
- Removed `context` from `googlecloud.NewPublisher`
