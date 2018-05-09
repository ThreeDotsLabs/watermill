package handler

import (
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

// todo - do it better way
func DecodeEventPayload(msg Message, target interface{}) error {
	if err := mapstructure.Decode(msg.Payload, target); err != nil {
		return errors.Wrap(err, "cannot decode payload")
	}

	return nil
}
