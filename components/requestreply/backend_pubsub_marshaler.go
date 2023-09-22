package requestreply

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

type BackendPubsubMarshaler[Response any] interface {
	MarshalReply(params BackendOnCommandProcessedParams[Response]) (*message.Message, error)
	UnmarshalReply(msg *message.Message) (reply CommandReply[Response], err error)
}

const (
	ErrorMetadataKey    = "_watermill_requestreply_error"
	HasErrorMetadataKey = "_watermill_requestreply_has_error"
	ResponseMetadataKey = "_watermill_requestreply_response"
)

type BackendPubsubJSONMarshaler[Response any] struct{}

func (m BackendPubsubJSONMarshaler[Response]) MarshalReply(
	params BackendOnCommandProcessedParams[Response],
) (*message.Message, error) {
	msg := message.NewMessage(watermill.NewUUID(), nil)

	if params.HandleErr != nil {
		msg.Metadata.Set(ErrorMetadataKey, params.HandleErr.Error())
		msg.Metadata.Set(HasErrorMetadataKey, "1")
	} else {
		msg.Metadata.Set(HasErrorMetadataKey, "0")
	}

	b, err := json.Marshal(params.HandlerResponse)
	if err != nil {
		return nil, errors.Wrap(err, "cannot marshal reply")
	}

	msg.Metadata.Set(ResponseMetadataKey, string(b))

	return msg, nil
}

func (m BackendPubsubJSONMarshaler[Response]) UnmarshalReply(msg *message.Message) (CommandReply[Response], error) {
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
