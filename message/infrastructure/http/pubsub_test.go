package http

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure"
)

const uuid_header = "Message-Uuid"
const metadata_header = "Message-Metadata"

type messageMetadata struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func createPubSub(t *testing.T) message.PubSub {
	logger := watermill.NewStdLogger(true, true)

	sub, err := NewSubscriber(":8080", unmarshalFn, logger)

	go func() {
		err := sub.StartHTTPServer()
		require.NoError(t, err)
	}()

	time.Sleep(200 * time.Millisecond)

	pub, err := NewPublisher(marshalFn, logger)
	require.NoError(t, err)

	return message.NewPubSub(pub, sub)
}

func unmarshalFn(topic string, req *http.Request) (*message.Message, error) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	uuid := req.Header.Get(uuid_header)
	if uuid == "" {
		return nil, errors.New("No UUID encoded in header")
	}
	msg := message.NewMessage(uuid, body)

	var metadata []messageMetadata
	err = json.Unmarshal([]byte(req.Header.Get(metadata_header)), &metadata)
	if err != nil {
		return nil, err
	}

	for _, item := range metadata {
		msg.Metadata.Set(item.Key, item.Value)
	}

	return msg, nil
}

func marshalFn(topic string, msg *message.Message) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodPost, "http://localhost:8080/"+topic, bytes.NewBuffer(msg.Payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set(uuid_header, msg.UUID)

	metadataItems := make([]messageMetadata, len(msg.Metadata))
	i := 0
	for key, val := range msg.Metadata {
		metadataItems[i] = messageMetadata{key, val}
		i++
	}
	b, err := json.Marshal(metadataItems)
	if err != nil {
		return nil, err
	}
	req.Header.Set(metadata_header, string(b))
	return req, nil
}

func TestPublishSubscribe(t *testing.T) {
	infrastructure.TestPubSub(
		t,
		infrastructure.Features{
			ConsumerGroups:      false,
			ExactlyOnceDelivery: true,
			GuaranteedOrder:     true,
			Persistent:          false,
		},
		createPubSub,
		nil,
	)
}
