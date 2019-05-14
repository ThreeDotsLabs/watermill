# UPGRADE FROM 0.3.x to 0.4

## `watermill/components/cqrs`

### `CommandHandler.HandlerName` and `EventHandler.HandlerName` was added to the interface.

If you are using metrics component, you may want to keep backward capability with handler names. In other cases, you can implement your own method of generating handler name.

Keeping backward capability for **event handlers**:

```
func (h CommandHandler) HandlerName() string {
    return fmt.Sprintf("command_processor-%s", h)
}
```

Keeping backward capability for **command handlers**:

```
func (h EventHandler) HandlerName() string {
    return fmt.Sprintf("event_processor-%s", ObjectName(h))
}
```

### Added `CommandsSubscriberConstructor` and `EventsSubscriberConstructor`

From now on, `CommandsSubscriberConstructor` and `EventsSubscriberConstructor` are passed to constructors in CQRS component.

They allow creating customized subscribers for every handler. For usage examples please check [_examples/cqrs-protobuf](_examples/cqrs-protobuf).


### Added context to `CommandHandler.Handle`, `CommandBus.Send`, `EventHandler.Handle` and `EventBus.Send`

Added missing context, which is passed to Publish function and handlers.

### Other

- `NewCommandProcessor` and `NewEventProcessor` now return an error instead of panic
- `DuplicateCommandHandlerError` is returned instead of panic when two handlers are handling the same command
- `CommandProcessor.routerHandlerFunc` and `EventProcessor.routerHandlerFunc` are now private
- using `GenerateCommandsTopic` and `GenerateEventsTopic` functions instead of constant topic to allow more flexibility


## `watermill/message/infrastructure/amqp`

### `Config.QueueBindConfig.RoutingKey` was replaced with `GenerateRoutingKey`

For backward compatibility, when using the constant value you should use a function:


```
func(topic string) string {
    return "routing_key"
}
```


## `message/router/middleware`

- `PoisonQueue` is now `PoisonQueue(pub message.Publisher, topic string) (message.HandlerMiddleware, error)`, not a struct


## `message/router.go`

- From now on, when all handlers are stopped, the router will also stop (`TestRouter_stop_when_all_handlers_stopped` test)
