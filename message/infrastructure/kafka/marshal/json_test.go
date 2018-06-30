package marshal_test

import (
	"testing"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/marshal"
	"github.com/roblaszczak/gooddd/message"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func TestJson(t *testing.T) {
	m := marshal.Json{}

	type payload struct {
		Foo string
	}

	msgPayload := payload{"bar"}
	msg := message.NewDefault(uuid.NewV4().String(), msgPayload)
	msg.SetMetadata("foo", "bar")

	producerMsg, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	confluentMsg := saramaMessageToConfluentMessage(producerMsg)

	unmarshaledMsg, err := m.Unmarshal(confluentMsg)
	require.NoError(t, err)

	assert.EqualValues(t, msg.UUID(), unmarshaledMsg.UUID())
	assert.EqualValues(t, msg.AllMetadata(), unmarshaledMsg.AllMetadata())

	unmarshaledPayload := payload{}
	unmarshaledMsg.UnmarshalPayload(&unmarshaledPayload)

	assert.Equal(t, msgPayload, unmarshaledPayload)
}

func TestJsonWithPartitioning(t *testing.T) {
	type payload struct {
		Foo string
		Key string
	}

	m := marshal.NewJsonWithPartitioning(func(topic string, msg message.Message) (string, error) {
		p := payload{}
		msg.UnmarshalPayload(&p)
		return p.Key, nil
	})

	msgPayload := payload{"bar", "1"}
	msg := message.NewDefault(uuid.NewV4().String(), msgPayload)
	msg.SetMetadata("foo", "bar")

	producerMsg, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	confluentMsg := saramaMessageToConfluentMessage(producerMsg)

	unmarshaledMsg, err := m.Unmarshal(confluentMsg)
	require.NoError(t, err)

	assert.EqualValues(t, msg.UUID(), unmarshaledMsg.UUID())
	assert.EqualValues(t, msg.AllMetadata(), unmarshaledMsg.AllMetadata())

	unmarshaledPayload := payload{}
	unmarshaledMsg.UnmarshalPayload(&unmarshaledPayload)

	assert.Equal(t, msgPayload, unmarshaledPayload)

	msgKey, err := producerMsg.Key.Encode()
	assert.NoError(t, err)
	assert.Equal(t, string(msgKey), msgPayload.Key)
}

func saramaMessageToConfluentMessage(producerMsg *sarama.ProducerMessage) *kafka.Message {
	value, _ := producerMsg.Value.Encode()

	var key []byte
	if producerMsg.Key != nil {
		key, _ = producerMsg.Key.Encode()
	}

	return &kafka.Message{
		Value: value,
		Key:   key,
	}
}
