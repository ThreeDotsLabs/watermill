package io_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/infrastructure/io"
)

func TestLosslessMarshaler(t *testing.T) {
	lm := io.LosslessMarshaler{}

	richMessage := message.NewMessage(watermill.NewUUID(), []byte("No bird soars too high, if he soars with his own wings"))
	richMessage.Metadata.Set("foo", "1")
	richMessage.Metadata.Set("bar", "2")

	wg := sync.WaitGroup{}
	wg.Add(100)

	once := sync.Once{}
	atLeastOneUnequal := false
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			sourceMessage := richMessage.Copy()
			sourceMessage.Metadata.Set("number", strconv.Itoa(i))

			b, err := lm.Marshal("topic", sourceMessage)
			require.NoError(t, err)

			unmarshaledMsg, err := lm.Unmarshal("other_topic", b)
			require.NoError(t, err, "expected to unmarshal correctly; topic should have no influence")

			if !sourceMessage.Equals(unmarshaledMsg) {
				//t.Logf("msg %+v not equal to source message %+v", unmarshaledMsg, sourceMessage)
				t.Logf("number metadata not equal: %s, expecting %s", unmarshaledMsg.Metadata.Get("number"), sourceMessage.Metadata.Get("number"))
				once.Do(func() {
					atLeastOneUnequal = true
				})
			}
		}(i)
	}

	wg.Wait()
	assert.False(t, atLeastOneUnequal, "expected all unmarshaled messages to equal the source message")
}
