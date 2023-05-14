package jsonendpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

const testTopic = "testTopic"

type testMessage struct {
	Title  string
	Number int
}

func (m *testMessage) ToBytes(t *testing.T) []byte {
	result, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("failed to encode to bytes: %v", err)
	}
	return result
}

func (m *testMessage) Validate() error {
	if m.Title == "" {
		return errors.New("title is required")
	}
	if m.Number == 0 {
		return errors.New("number is required")
	}
	return nil
}

func TestEndpointCreationIntegration(t *testing.T) {
	pubSub := gochannel.NewGoChannel(gochannel.Config{
		OutputChannelBuffer: 100,
		Persistent:          false,
	},
		watermill.NewStdLogger(true, true),
	)

	m := &testMessage{
		Title:  "title",
		Number: 9,
	}

	// err := pubSub.Publish(testTopic, m)
	// if err != nil {
	// 	t.Fatalf("cannot publish the test message: %v", err)
	// }

	messages, err := pubSub.Subscribe(context.Background(), testTopic)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	endpoint := New(999999, func(m *testMessage) (*message.Message, error) {
		payload, err := json.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("failed to encode: %w", err)
		}
		return message.NewMessage(watermill.NewUUID(), payload), nil
	}, testTopic, pubSub)

	request := httptest.NewRequest(
		http.MethodPost,
		"/url/path",
		bytes.NewReader(m.ToBytes(t)),
	)
	w := httptest.NewRecorder()
	endpoint(w, request)

	if err = validateEndpointResponse(w.Result()); err != nil {
		t.Fatalf("HTTP test request failed: %v", err)
	}

	replayedMessage := <-messages
	var rm *testMessage
	if err = json.Unmarshal(replayedMessage.Payload, &rm); err != nil {
		t.Fatalf("cannot decode replayed message: %v", err)
	}

	if rm.Title != m.Title {
		t.Fatalf("title mismatch: %q vs %q", rm.Title, rm.Title)
	}
	if rm.Number != m.Number {
		t.Fatalf("number mismatch: %q vs %q", rm.Number, rm.Number)
	}
}

func validateEndpointResponse(response *http.Response) (err error) {
	defer response.Body.Close()

	data, err := ioutil.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP test request failed: status code is not OK: %d", response.StatusCode)
	}

	var values map[string]string
	if err = json.Unmarshal(data, &values); err != nil {
		return fmt.Errorf("JSON decoding failure for %q: %w", data, err)
	}
	if _, ok := values["UUID"]; !ok {
		return errors.New("response does not contain new message UUID")
	}
	return nil
}
