package kafka_test

import (
	"testing"

	"github.com/Shopify/sarama"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/kafka"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultMarshaler_MarshalUnmarshal(t *testing.T) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.UUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(producerToConsumerMessage(marshaled))
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))
}

func BenchmarkDefaultMarshaler_Marshal(b *testing.B) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.UUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	for i := 0; i < b.N; i++ {
		m.Marshal("foo", msg)
	}
}

func BenchmarkDefaultMarshaler_Unmarshal(b *testing.B) {
	m := kafka.DefaultMarshaler{}

	msg := message.NewMessage(watermill.UUID(), []byte("payload"))
	msg.Metadata.Set("foo", "bar")

	marshaled, err := m.Marshal("foo", msg)
	if err != nil {
		b.Fatal(err)
	}

	consumedMsg := producerToConsumerMessage(marshaled)

	for i := 0; i < b.N; i++ {
		m.Unmarshal(consumedMsg)
	}
}

func TestWithPartitioningMarshaler_MarshalUnmarshal(t *testing.T) {
	m := kafka.NewWithPartitioningMarshaler(func(topic string, msg *message.Message) (string, error) {
		return msg.Metadata.Get("partition"), nil
	})

	partitionKey := "1"
	msg := message.NewMessage(watermill.UUID(), []byte("payload"))
	msg.Metadata.Set("partition", partitionKey)

	producerMsg, err := m.Marshal("topic", msg)
	require.NoError(t, err)

	unmarshaledMsg, err := m.Unmarshal(producerToConsumerMessage(producerMsg))
	require.NoError(t, err)

	assert.True(t, msg.Equals(unmarshaledMsg))

	assert.NoError(t, err)

	producerKey, err := producerMsg.Key.Encode()
	require.NoError(t, err)

	assert.Equal(t, string(producerKey), partitionKey)
}

func producerToConsumerMessage(producerMessage *sarama.ProducerMessage) *sarama.ConsumerMessage {
	var key []byte

	if producerMessage.Key != nil {
		var err error
		key, err = producerMessage.Key.Encode()
		if err != nil {
			panic(err)
		}
	}

	var value []byte
	if producerMessage.Value != nil {
		var err error
		value, err = producerMessage.Value.Encode()
		if err != nil {
			panic(err)
		}
	}

	var headers []*sarama.RecordHeader
	for i := range producerMessage.Headers {
		headers = append(headers, &producerMessage.Headers[i])
	}

	return &sarama.ConsumerMessage{
		Key:       key,
		Value:     value,
		Topic:     producerMessage.Topic,
		Partition: producerMessage.Partition,
		Offset:    producerMessage.Offset,
		Timestamp: producerMessage.Timestamp,
		Headers:   headers,
	}
}
