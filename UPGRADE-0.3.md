# UPGRADE FROM 0.2.x to 0.3

# `watermill/message`

- `message.Message.Ack` and `message.Message.Nack` now return `bool` instead of `error`
- `message.Subscriber.Subscribe` now accepts `context.Context` as the first argument
- `message.Subscriber.Subscribe` now returns `<-chan *Message` instead of `chan *Message`
- `message.Router.AddHandler` and `message.Router.AddNoPublisherHandler` now panic, instead of returning error

# `watermill/message/infrastructure`

- updated all Pub/Subs to new `message.Subscriber` interface
- `gochannel.NewGoChannel` now accepts `gochannel.Config`, instead of positional parameters
- `http.NewSubscriber` now accepts `http.SubscriberConfig`, instead of positional parameters

# `watermill/message/router/middleware`

- `metrics.NewMetrics` is removed, please use the [metrics](components/metrics) component instead

# `watermill`

- `watermill.LoggerAdapter` interface now requires a `With(fields LogFields) LoggerAdapter` method
