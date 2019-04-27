# UPGRADE FROM 0.3.x to 0.4

# `watermill/components/cqrs`

### `CommandHandler.HandlerName` and `EventHandler.HandlerName` was added to the interface.

If you are using metrics component, you may want to keep backward capability with handler names. In other case you can implement your own method of generating handler name.

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
