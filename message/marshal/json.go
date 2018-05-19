package marshal

import (
	"github.com/roblaszczak/gooddd/message"
	"encoding/json"
)

func Json(message *message.Message) ([]byte, error) {
	return json.Marshal(message)
}

func UnmarshalJson(data []byte, msg *message.Message) error {
	return json.Unmarshal(data, msg)
}
