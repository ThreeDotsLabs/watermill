package marshal_test

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/roblaszczak/gooddd/message"
	"github.com/roblaszczak/gooddd/message/infrastructure/kafka/marshal"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJson(t *testing.T) {
	m := marshal.ConfluentKafka{}

	msg := message.NewMessage(uuid.NewV4().String(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(marshaled)
	require.NoError(t, err)

	assert.EqualValues(t, msg.UUID, unmarshaledMsg.UUID)
	assert.EqualValues(t, msg.Metadata, unmarshaledMsg.Metadata)
	assert.EqualValues(t, msg.Payload, unmarshaledMsg.Payload)
}

func TestJsonWithPartitioning(t *testing.T) {
	// todo - fix
	//type payload struct {
	//	Foo string
	//	Key string
	//}
	//
	//m := marshal.NewJsonWithPartitioning(func(topic string, msg message.Message) (string, error) {
	//	p := msg.Payload().(payload)
	//	return p.Key, nil
	//})
	//
	//msgPayload := payload{"bar", "1"}
	//msg := message.NewMessage(uuid.NewV4().String(), msgPayload)
	//msg.SetMetadata("foo", "bar")
	//
	//producerMsg, err := m.Marshal("topic", msg)
	//require.NoError(t, err)
	//
	//confluentMsg := saramaMessageToConfluentMessage(producerMsg)
	//
	//unmarshaledMsg, err := m.Unmarshal(confluentMsg)
	//require.NoError(t, err)
	//
	//assert.EqualValues(t, msg.UUID(), unmarshaledMsg.UUID())
	//assert.EqualValues(t, msg.AllMetadata(), unmarshaledMsg.AllMetadata())
	//
	//unmarshaledPayload := payload{}
	//unmarshaledMsg.UnmarshalPayload(&unmarshaledPayload)
	//
	//assert.Equal(t, msgPayload, unmarshaledPayload)
	//
	//msgKey, err := producerMsg.Key.Encode()
	//assert.NoError(t, err)
	//assert.Equal(t, string(msgKey), msgPayload.Key)
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
