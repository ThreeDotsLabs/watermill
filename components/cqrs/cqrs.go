package cqrs

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
)

// todo - link to docs
// todo - glossary and schema

type FacadeConfig struct {
	CommandsTopic   string
	CommandHandlers func(commandBus *CommandBus, eventBus *EventBus) []CommandHandler
	CommandsPubSub  message.PubSub

	EventsTopic   string
	EventHandlers func(commandBus *CommandBus, eventBus *EventBus) []EventHandler
	EventsPubSub  message.PubSub

	Router                *message.Router
	Logger                watermill.LoggerAdapter
	CommandEventMarshaler CommandEventMarshaler
}

func (c FacadeConfig) Validate() error {
	var err error

	if c.CommandsEnabled() {
		if c.CommandsTopic == "" {
			err = multierror.Append(err, errors.New("CommandsTopic is empty"))
		}
		if c.CommandsPubSub == nil {
			err = multierror.Append(err, errors.New("CommandsPubSub is nil"))
		}
	}
	if c.EventsEnabled() {
		if c.EventsTopic == "" {
			err = multierror.Append(err, errors.New("EventsTopic is empty"))
		}
		if c.EventsPubSub == nil {
			err = multierror.Append(err, errors.New("EventsPubSub is nil"))
		}
	}

	if c.Router == nil {
		err = multierror.Append(err, errors.New("Router is nil"))
	}
	if c.Logger == nil {
		err = multierror.Append(err, errors.New("Logger is nil"))
	}
	if c.CommandEventMarshaler == nil {
		err = multierror.Append(err, errors.New("CommandEventMarshaler is nil"))
	}

	return err
}

func (c FacadeConfig) EventsEnabled() bool {
	return c.EventsTopic != "" || c.EventsPubSub != nil
}

func (c FacadeConfig) CommandsEnabled() bool {
	return c.CommandsTopic != "" || c.CommandsPubSub != nil
}

type Facade struct {
	commandsTopic string
	commandBus    *CommandBus

	eventsTopic string
	eventBus    *EventBus

	commandEventMarshaler CommandEventMarshaler
}

func (f Facade) CommandsTopic() string {
	return f.commandsTopic
}

func (f Facade) CommandBus() *CommandBus {
	return f.commandBus
}

func (f Facade) EventsTopic() string {
	return f.eventsTopic
}

func (f Facade) EventBus() *EventBus {
	return f.eventBus
}

func (f Facade) CommandEventMarshaler() CommandEventMarshaler {
	return f.commandEventMarshaler
}

func NewFacade(config FacadeConfig) (*Facade, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}

	c := &Facade{
		commandsTopic:         config.CommandsTopic,
		eventsTopic:           config.EventsTopic,
		commandEventMarshaler: config.CommandEventMarshaler,
	}

	if config.CommandsEnabled() {
		c.commandBus = NewCommandBus(config.CommandsPubSub, config.CommandsTopic, config.CommandEventMarshaler)
	} else {
		config.Logger.Info("Empty CommandsTopic, command bus will be not created", nil)
	}
	if config.EventsEnabled() {
		c.eventBus = NewEventBus(config.EventsPubSub, config.EventsTopic, config.CommandEventMarshaler)
	} else {
		config.Logger.Info("Empty EventsTopic, event bus will be not created", nil)
	}

	if config.CommandHandlers != nil {
		commandProcessor := NewCommandProcessor(
			config.CommandHandlers(c.commandBus, c.eventBus),
			config.CommandsTopic,
			config.CommandsPubSub,
			config.CommandEventMarshaler,
			config.Logger,
		)

		err := commandProcessor.AddHandlersToRouter(config.Router)
		if err != nil {
			return nil, err
		}
	}
	if config.EventHandlers != nil {
		eventProcessor := NewEventProcessor(
			config.EventHandlers(c.commandBus, c.eventBus),
			config.EventsTopic,
			config.EventsPubSub,
			config.CommandEventMarshaler,
			config.Logger,
		)

		err := eventProcessor.AddHandlersToRouter(config.Router)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}
