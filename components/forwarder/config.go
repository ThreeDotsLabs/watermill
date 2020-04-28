package forwarder

type Config struct {
	// ForwarderTopic is a topic on which the forwarder will be listening to enveloped messages to forward.
	ForwarderTopic string
}
