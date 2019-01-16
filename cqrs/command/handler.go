package command

type Handler interface {
	NewCommand() interface{}
	Handle(cmd interface{}) error
}
