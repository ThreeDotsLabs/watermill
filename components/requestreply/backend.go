package requestreply

import (
	"context"
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type Backend[Response any] interface {
	//ModifyCommandMessageBeforePublish(cmdMsg *message.Message, command any) error

	ListenForNotifications(ctx context.Context, params ListenForNotificationsParams) (<-chan CommandReply[Response], error)

	OnCommandProcessed(ctx context.Context, params OnCommandProcessedParams[Response]) error
}

type ListenForNotificationsParams struct {
	Command        any
	NotificationID string
}

type OnCommandProcessedParams[Response any] struct {
	Cmd    any
	CmdMsg *message.Message
	// todo: doc when present
	HandlerResponse Response
	HandleErr       error
}

// todo: rename? it's only for pubsub backend?
type Marshaler[Response any] interface {
	MarshalReply(params OnCommandProcessedParams[Response], reply CommandReply[Response]) (*message.Message, error)
	UnmarshalReply(msg *message.Message) (reply CommandReply[Response], err error)
}

const (
	ErrorMetadataKey    = "_watermill_requestreply_error"
	HasErrorMetadataKey = "_watermill_requestreply_has_error"
	ResponseMetadataKey = "_watermill_requestreply_response"
)

// todo: rename?
type JsonMarshaler[Response any] struct {
}

func NewJsonMarshaler[Response any]() JsonMarshaler[Response] {
	return JsonMarshaler[Response]{}
}

func (j JsonMarshaler[Response]) MarshalReply(
	params OnCommandProcessedParams[Response],
	reply CommandReply[Response],
) (*message.Message, error) {
	msg := message.NewMessage(watermill.NewUUID(), nil)

	if params.HandleErr != nil {
		msg.Metadata.Set(ErrorMetadataKey, params.HandleErr.Error())
		msg.Metadata.Set(HasErrorMetadataKey, "1")
	} else {
		msg.Metadata.Set(HasErrorMetadataKey, "0")
	}

	b, err := json.Marshal(reply.HandlerResponse)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal reply")
	}

	msg.Metadata.Set(ResponseMetadataKey, string(b))

	return msg, nil
}

func (j JsonMarshaler[Response]) UnmarshalReply(msg *message.Message) (CommandReply[Response], error) {
	reply := CommandReply[Response]{}

	if msg.Metadata.Get(HasErrorMetadataKey) == "1" {
		reply.HandlerErr = errors.New(msg.Metadata.Get(ErrorMetadataKey))
	}

	var response Response
	if err := json.Unmarshal([]byte(msg.Metadata.Get(ResponseMetadataKey)), &response); err != nil {
		return CommandReply[Response]{}, errors.Wrap(err, "cannot unmarshal response")
	}
	reply.HandlerResponse = response

	return reply, nil
}
