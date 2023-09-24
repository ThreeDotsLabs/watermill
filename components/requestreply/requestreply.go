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
	// HandlerResult contains the handler result.
	// It's preset only when NewCommandHandlerWithResult is used. If NewCommandHandler is used, HandlerResult is empty.
	//
	// Result is sent even if the handler returns an error.
	HandlerResult Result

	// Error contains the error returned by the command handler or the Backend when handling notification fails.
	// Handling the notification can fail, for example, when unmarshaling the message or if there's a timeout.
	// If listening for a reply times out or the context is canceled, the Error is ReplyTimeoutError.
	//
	// If an error from the handler is returned, CommandHandlerError is returned.
	// If processing was successful, Error is nil.
	Error error

	// NotificationMessage contains the notification message sent after the command is handled.
	// It's present only if the request/reply backend uses a Pub/Sub for notifications (for example, PubSubBackend).
	//
	// Warning: NotificationMessage is nil if a timeout occurs.
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
// It correlates commands with replies between the bus and the handler.
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
