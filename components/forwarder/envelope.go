package forwarder

import (
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/pkg/errors"
)

// messageEnvelope wraps Watermill message and contains destination topic.
type messageEnvelope struct {
	DestinationTopic string `json:"destination_topic"`

	UUID     string            `json:"uuid"`
	Payload  []byte            `json:"payload"`
	Metadata map[string]string `json:"metadata"`
}

func wrapMessageInEnvelope(destinationTopic string, msg *message.Message) (*message.Message, error) {
	envelope := messageEnvelope{
		DestinationTopic: destinationTopic,
		UUID:             msg.UUID,
		Payload:          msg.Payload,
		Metadata:         msg.Metadata,
	}

	envelopedMessage, err := json.Marshal(envelope)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal message")
	}

	return message.NewMessage(watermill.NewUUID(), envelopedMessage), nil
}

func unwrapMessageFromEnvelope(msg *message.Message) (destinationTopic string, unwrappedMsg *message.Message, err error) {
	envelopedMsg := messageEnvelope{}
	if err := json.Unmarshal(msg.Payload, &envelopedMsg); err != nil {
		return "", nil, errors.Wrap(err, "cannot unmarshal message wrapped in envelope")
	}

	watermillMessage := message.NewMessage(envelopedMsg.UUID, envelopedMsg.Payload)
	watermillMessage.Metadata = envelopedMsg.Metadata

	return envelopedMsg.DestinationTopic, watermillMessage, nil
}
