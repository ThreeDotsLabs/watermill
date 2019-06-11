package cqrs

// Topic returns a generator function for a single topic name.
func Topic(topic string) func(string) string {
	return func(string) string {
		return topic
	}
}

// MessageTopic is a generator function returning the message name as a topic.
func MessageTopic(topic string) string {
	return topic
}
