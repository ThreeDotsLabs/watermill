# UPGRADE FROM 0.2.x to 0.3

# `message` package

- `message.Message.Ack` and `message.Message.Nack` now returns `bool` instead of `error`
- `message.Subscriber.Subscribe` now accepts `context.Context` as the first argument
- `message.Subscriber.Subscribe` now returns `<-chan *Message` instead of `chan *Message`
- `message.Router.AddHandler` and `message.Router.AddNoPublisherHandler` now panics instead of returning error

# `message/infrastructure` package

- updated all Pub/Subs to new `message.Subscriber` interface
- `gochannel.NewGoChannel` now accepts config, instead of positional parameters
- `http.NewSubscriber` now accepts config instead of positional parameters

# `message/router/middleware` package

- `metrics.NewMetrics` is removed, please use [metrics](components/metrics) component instead

# `watermill` package

- `watermill.LoggerAdapter` interface now requires `With(fields LogFields) LoggerAdapter` method
