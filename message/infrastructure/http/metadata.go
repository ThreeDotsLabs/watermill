package http

import (
	"encoding/json"
	"net/http"

	"github.com/ThreeDotsLabs/watermill/message"
)

type messageMetadata struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func metadataToJson(msg *message.Message) ([]byte, error) {
	metadataItems := make([]messageMetadata, len(msg.Metadata))
	i := 0
	for key, val := range msg.Metadata {
		metadataItems[i] = messageMetadata{key, val}
		i++
	}
	return json.Marshal(metadataItems)
}

func metadataFromRequest(req *http.Request) (message.Metadata, error) {
	var metadataItems []messageMetadata
	err := json.Unmarshal([]byte(req.Header.Get(HeaderMetadata)), &metadataItems)
	if err != nil {
		return nil, err
	}

	metadata := message.Metadata{}

	for _, item := range metadataItems {
		metadata.Set(item.Key, item.Value)
	}

	return metadata, nil
}
