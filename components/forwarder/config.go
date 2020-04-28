package forwarder

type Config struct {
	// ForwarderTopic is a topic on which the forwarder will be listening to enveloped messages to forward.
	// Defaults to `forwarder_topic`.
	ForwarderTopic string
}

func (c *Config) setDefaults() {
	if c.ForwarderTopic == "" {
		c.ForwarderTopic = "forwarder_topic"
	}
}
