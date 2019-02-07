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
	CommandHandlers func(commandBus CommandBus, eventBus EventBus) []CommandHandler

	EventsTopic   string
	EventHandlers func(commandBus CommandBus, eventBus EventBus) []EventHandler

	Router                *message.Router
	PubSub                message.PubSub
	Logger                watermill.LoggerAdapter
	CommandEventMarshaler CommandEventMarshaler
}

func (c FacadeConfig) Validate() error {
	var err error

	if c.CommandsTopic != "" && c.CommandHandlers == nil {
		err = multierror.Append(err, errors.New("CommandHandlers is nil"))
	}
	if c.EventsTopic != "" && c.EventHandlers == nil {
		err = multierror.Append(err, errors.New("EventHandlers is nil"))
	}

	if c.Router == nil {
		err = multierror.Append(err, errors.New("Router is nil"))
	}
	if c.PubSub == nil {
		err = multierror.Append(err, errors.New("PubSub is nil"))
	}
	if c.Logger == nil {
		err = multierror.Append(err, errors.New("Logger is nil"))
	}
	if c.CommandEventMarshaler == nil {
		err = multierror.Append(err, errors.New("CommandEventMarshaler is nil"))
	}

	return err
}

type Facade struct {
	commandsTopic string
	commandBus    CommandBus

	eventsTopic string
	eventBus    EventBus

	commandEventMarshaler CommandEventMarshaler
}

func (f Facade) CommandsTopic() string {
	return f.commandsTopic
}

func (f Facade) CommandBus() CommandBus {
	return f.commandBus
}

func (f Facade) EventsTopic() string {
	return f.eventsTopic
}

func (f Facade) EventBus() EventBus {
	return f.eventBus
}

func (f Facade) CommandEventMarshaler() CommandEventMarshaler {
	return f.commandEventMarshaler
}

func NewFacade(config FacadeConfig) (Facade, error) {
	if err := config.Validate(); err != nil {
		return Facade{}, errors.Wrap(err, "invalid config")
	}

	c := Facade{
		commandsTopic:         config.CommandsTopic,
		eventsTopic:           config.EventsTopic,
		commandEventMarshaler: config.CommandEventMarshaler,
	}

	if config.CommandsTopic != "" {
		c.commandBus = NewCommandBus(config.PubSub, config.CommandsTopic, config.CommandEventMarshaler)

		commandProcessor := NewCommandProcessor(
			config.CommandHandlers(c.commandBus, c.eventBus),
			config.CommandsTopic,
			config.PubSub,
			config.CommandEventMarshaler,
			config.Logger,
		)

		err := commandProcessor.AddHandlersToRouter(config.Router)
		if err != nil {
			return Facade{}, err
		}
	} else {
		config.Logger.Info("Empty CommandsTopic, command bus will be not created", nil)
	}

	if config.EventsTopic != "" {
		c.eventBus = NewEventBus(config.PubSub, config.EventsTopic, config.CommandEventMarshaler)

		eventProcessor := NewEventProcessor(
			config.EventHandlers(c.commandBus, c.eventBus),
			config.EventsTopic,
			config.PubSub,
			config.CommandEventMarshaler,
			config.Logger,
		)

		err := eventProcessor.AddHandlersToRouter(config.Router)
		if err != nil {
			return Facade{}, err
		}
	} else {
		config.Logger.Info("Empty EventsTopic, event bus will be not created", nil)
	}

	return c, nil
}
