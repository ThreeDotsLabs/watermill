package requestreply

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

// NoResult is a result type for commands that don't have result.
type NoResult = struct{}

type Reply[Result any] struct {
	// HandlerResult contains handler result.
	// It's preset only when NewCommandHandlerWithResult was used. If NewCommandHandler was used, HandlerResult will be empty.
	//
	// Result is sent even if handler returned error.
	HandlerResult Result

	// Error contains the error returned by the command handler or by Backend when handling notification failed.
	// Handling notification can fail for example during unmarshaling message or timeout.
	// If listening for reply timed out or context was canceled, Error is ReplyTimeoutError.
	//
	// If error from handler is returned, CommandHandlerError is returned.
	// If processing was successful, Error is nil.
	Error error

	// NotificationMessage contains the notification message send after command is handled.
	// It's only present if the request/reply backend is using Pub/Sub for notifications (for example: PubSubBackend).
	//
	// Warning: NotificationMessage is nil if timeout occurred.
	NotificationMessage *message.Message
}

type Backend[Result any] interface {
	ListenForNotifications(ctx context.Context, params BackendListenForNotificationsParams) (<-chan Reply[Result], error)
	OnCommandProcessed(ctx context.Context, params BackendOnCommandProcessedParams[Result]) error
}

type BackendListenForNotificationsParams struct {
	Command     any
	OperationID OperationID
}

type BackendOnCommandProcessedParams[Result any] struct {
	Command        any
	CommandMessage *message.Message

	HandlerResult Result
	HandleErr     error
}

// OperationID is a unique identifier of a command.
// It's used to correlate command with reply between bus and handler.
type OperationID string

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
