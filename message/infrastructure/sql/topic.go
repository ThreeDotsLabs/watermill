package sql

import (
	"regexp"

	"github.com/pkg/errors"
)

var disallowedTopicCharacters = regexp.MustCompile(`[^A-Za-z0-9\-\$\:\.\_]`)

var ErrInvalidTopicName = errors.New("topic name should not contain characters matched by " + disallowedTopicCharacters.String())

// validateTopicName checks if the topic name contains any characters which could be unsuitable for the SQL Pub/Sub.
// Topics are translated into SQL tables and patched into some queries, so this is done to prevent injection as well.
func validateTopicName(topic string) error {
	if disallowedTopicCharacters.MatchString(topic) {
		return errors.Wrap(ErrInvalidTopicName, topic)
	}

	return nil
}
