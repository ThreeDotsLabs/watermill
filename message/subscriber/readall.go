package subscriber

import (
	"github.com/roblaszczak/gooddd/message"
	"time"
	"github.com/pkg/errors"
)

// todo - test
func ReadAll(messagesCh <-chan message.Message, expectedCount int, timeout time.Duration) ([]message.Message, error) {
	var receivedMessages []message.Message

	allMessagesReceived := make(chan struct{}, 1)
	go func() {
		for msg := range messagesCh {
			receivedMessages = append(receivedMessages, msg)
			msg.Acknowledge()

			if len(receivedMessages) == expectedCount {
				allMessagesReceived <- struct{}{}
				break
			}
		}

	}()

	select {
	case <-allMessagesReceived:
	case <-time.After(timeout):
		return receivedMessages, errors.Errorf(
			"waiting for messages timeouted, received %d of %d messages",
			len(receivedMessages), expectedCount,
		)
	}

	return receivedMessages, nil
}
