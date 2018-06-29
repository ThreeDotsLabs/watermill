package subscriber

import (
	"github.com/roblaszczak/gooddd/message"
	"time"
)

// todo - test
func BulkRead(messagesCh <-chan message.Message, expectedCount int, timeout time.Duration) (receivedMessages []message.Message, all bool) {
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
		return receivedMessages, false
	}

	return receivedMessages, true
}
