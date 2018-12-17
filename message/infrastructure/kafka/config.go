package kafka

import "github.com/Shopify/sarama"

// DefaultSaramaSubscriberConfig creates default Sarama config used by Watermill.
//
// Custom config can be passed to NewSubscriber and NewPublisher.
//
//		saramaConfig := DefaultSaramaSubscriberConfig()
//		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
//
//		subscriber, err := NewSubscriber(watermillConfig, saramaConfig, unmarshaler, logger)
//		// ...
//
func DefaultSaramaSubscriberConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Consumer.Return.Errors = true
	config.ClientID = "watermill"

	return config
}
