package requestreply

import (
	"fmt"
	"time"
)

// ReplyTimeoutError is returned when the reply timeout is exceeded.
type ReplyTimeoutError struct {
	Duration time.Duration
	Err      error
}

func (e ReplyTimeoutError) Error() string {
	return fmt.Sprintf("reply timeout after %s: %s", e.Duration, e.Err)
}

type ReplyUnmarshalError struct {
	Err error
}

func (r ReplyUnmarshalError) Error() string {
	return fmt.Sprintf("cannot unmarshal reply: %s", r.Err)
}

func (r ReplyUnmarshalError) Unwrap() error {
	return r.Err
}

// CommandHandlerError is returned when the command handler returns an error.
type CommandHandlerError struct {
	Err error
}

func (e CommandHandlerError) Error() string {
	return e.Err.Error()
}

func (e CommandHandlerError) Unwrap() error {
	return e.Err
}
