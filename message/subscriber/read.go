package subscriber

import (
	"github.com/roblaszczak/gooddd/message"
	"time"
)

func BulkRead(messagesCh <-chan message.Message, limit int, timeout time.Duration) (receivedMessages []message.Message, all bool) {
	allMessagesReceived := make(chan struct{}, 1)

	go func() {
		for msg := range messagesCh {
			receivedMessages = append(receivedMessages, msg)
			msg.Acknowledge()

			if len(receivedMessages) == limit {
				allMessagesReceived <- struct{}{}
				break
			}
		}
		// messagesCh closed
		allMessagesReceived <- struct{}{}
	}()

	select {
	case <-allMessagesReceived:
	case <-time.After(timeout):
	}

	return receivedMessages, len(receivedMessages) == limit
}
