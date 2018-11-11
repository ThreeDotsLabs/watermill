package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

func mergeConfluentConfigs(baseConfig *kafka.ConfigMap, valuesToSet kafka.ConfigMap) error {
	for key, value := range valuesToSet {
		if err := baseConfig.SetKey(key, value); err != nil {
			return errors.Wrapf(err, "cannot overwrite config value for %s", key)
		}
	}

	return nil
}
