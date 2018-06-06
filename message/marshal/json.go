package marshal

import (
	"github.com/roblaszczak/gooddd/message"
	"encoding/json"
)

func Json(message message.Message) ([]byte, error) {
	return json.Marshal(message)
}

func UnmarshalJson(data []byte) (message.Message, error) {
	msg := message.NewEmptyDefault()
	if err := json.Unmarshal(data, msg); err != nil {
		return nil, err
	}

	return msg, nil
}
