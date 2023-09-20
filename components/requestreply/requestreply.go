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
