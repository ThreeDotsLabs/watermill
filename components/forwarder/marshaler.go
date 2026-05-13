package forwarder

import "encoding/json"

// Marshaler is used by the Forwarder Publisher to serialize the envelope sent
// to the forwarder topic, and by the Forwarder consumer to deserialize it.
//
// The interface matches the signatures of encoding/json's Marshal and Unmarshal,
// so any drop-in JSON library (e.g. github.com/bytedance/sonic, github.com/goccy/go-json)
// can be adapted with a small wrapper.
//
// The default is DefaultMarshaler, which uses encoding/json.
type Marshaler interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// DefaultMarshaler uses the standard library's encoding/json package.
type DefaultMarshaler struct{}

func (DefaultMarshaler) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (DefaultMarshaler) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
