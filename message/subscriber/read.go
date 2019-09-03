package subscriber

import (
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
)

func BulkRead(messagesCh <-chan *message.Message, limit int, timeout time.Duration) (receivedMessages message.Messages, all bool) {
MessagesLoop:
	for len(receivedMessages) < limit {
		select {
		case msg, ok := <-messagesCh:
			if !ok {
				break MessagesLoop
			}

			receivedMessages = append(receivedMessages, msg)
			msg.Ack()
		case <-time.After(timeout):
			break MessagesLoop
		}
	}

	return receivedMessages, len(receivedMessages) == limit
}

func BulkReadWithDeduplication(messagesCh <-chan *message.Message, limit int, timeout time.Duration) (receivedMessages message.Messages, all bool) {
	receivedIDs := map[string]struct{}{}

MessagesLoop:
	for len(receivedMessages) < limit {
		select {
		case msg, ok := <-messagesCh:
			if !ok {
				break MessagesLoop
			}

			if _, ok := receivedIDs[msg.UUID]; !ok {
				receivedIDs[msg.UUID] = struct{}{}
				receivedMessages = append(receivedMessages, msg)
			}
			msg.Ack()
		case <-time.After(timeout):
			break MessagesLoop
		}
	}

	return receivedMessages, len(receivedMessages) == limit
}
