package io_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/io"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var errWritingOnClosedWriter = errors.New("mockWriter is closed")

type mockWriter struct {
	messages [][]byte
	closed   bool
}

func (m *mockWriter) Write(p []byte) (n int, err error) {
	if m.closed {
		return 0, errWritingOnClosedWriter
	}

	m.messages = append(m.messages, p)
	return len(p), nil
}

func (m *mockWriter) Close() error {
	m.closed = true
	return nil
}

func TestPublisher_Publish(t *testing.T) {
	mw := &mockWriter{
		messages: [][]byte{},
	}
	pub, err := io.NewPublisher(
		mw,
		io.PublisherConfig{
			MarshalFunc: io.PayloadMarshalFunc,
		},
	)
	require.NoError(t, err)

	msg1 := message.NewMessage("1", []byte("foo"))
	msg2 := message.NewMessage("2", []byte("bar"))
	msg3 := message.NewMessage("3", []byte("baz"))
	msg4 := message.NewMessage("4", []byte("bax"))

	err = pub.Publish(
		"topic",
		msg1,
		msg2,
	)
	require.NoError(t, err)

	err = pub.Publish(
		"another_topic",
		msg3,
	)
	require.NoError(t, err)

	err = pub.Close()
	require.NoError(t, err)

	err = pub.Publish(
		"another_topic",
		msg4,
	)
	require.Error(t, err, "should not successfully publish on closed publisher")
	assert.NotEqual(t, errWritingOnClosedWriter, errors.Cause(err), "should not have called Write, but refused instead")

	require.Len(t, mw.messages, 3, "expected to record 3 messages")

	assert.Equal(t, append([]byte(msg1.Payload), '\n'), mw.messages[0])
	assert.Equal(t, append([]byte(msg2.Payload), '\n'), mw.messages[1])
	assert.Equal(t, append([]byte(msg3.Payload), '\n'), mw.messages[2])
}
