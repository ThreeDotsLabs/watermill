package handler

import (
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/roblaszczak/gooddd/message"
)

// todo - do it better way
func DecodeEventPayload(msg *message.Message, target interface{}) error {
	if err := mapstructure.Decode(msg.Payload, target); err != nil {
		return errors.Wrap(err, "cannot decode payload")
	}

	return nil
}
