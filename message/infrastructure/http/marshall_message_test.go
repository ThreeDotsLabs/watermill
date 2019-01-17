package http_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill/message"
	watermill_http "github.com/ThreeDotsLabs/watermill/message/infrastructure/http"
)

var (
	host        = "some-server.domain"
	marshalFunc = watermill_http.DefaultMarshalMessageFunc("http://" + host)

	metadataKey   = "key"
	metadataValue = "value"
	msgUUID       = "1"
	msgPayload    = []byte("payload")
	msg           *message.Message
)

func init() {
	msg = message.NewMessage(msgUUID, msgPayload)
	msg.Metadata.Set(metadataKey, metadataValue)
}

func TestDefaultMarshalMessageFunc(t *testing.T) {
	topic := "test_topic"
	req, err := marshalFunc(topic, msg)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, req.Method)
	assert.Equal(t, msgUUID, req.Header.Get(watermill_http.HeaderUUID))

	assert.Equal(t, "http", req.URL.Scheme)
	assert.Equal(t, host, req.URL.Host)
	assert.Equal(t, topic, strings.TrimPrefix(req.URL.Path, "/"))

	body, err := ioutil.ReadAll(req.Body)
	require.NoError(t, err)
	assert.Equal(t, msgPayload, body)

	var metadata message.Metadata
	err = json.Unmarshal([]byte(req.Header.Get(watermill_http.HeaderMetadata)), &metadata)
	require.NoError(t, err)
	assert.Equal(t, metadataValue, metadata.Get(metadataKey))
}
