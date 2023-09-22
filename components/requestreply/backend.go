package requestreply

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

type Backend[Response any] interface {
	ListenForNotifications(ctx context.Context, params BackendListenForNotificationsParams) (<-chan CommandReply[Response], error)
	OnCommandProcessed(ctx context.Context, params BackendOnCommandProcessedParams[Response]) error
}

type BackendListenForNotificationsParams struct {
	Command        any
	NotificationID string
}

type BackendOnCommandProcessedParams[Response any] struct {
	Command        any
	CommandMessage *message.Message

	// todo: doc when present
	HandlerResponse Response
	HandleErr       error
}
