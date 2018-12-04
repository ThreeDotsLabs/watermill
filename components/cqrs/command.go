package cqrs

type Command interface {
	CommandName() string
}
