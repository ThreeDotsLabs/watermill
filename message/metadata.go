package message

// Metadata is sent with every message to provide extra context without unmarshaling the message payload.
type Metadata map[string]string

// Get returns the metadata value for the given key. If the key is not found, an empty string is returned.
func (m Metadata) Get(key string) string {
	if v, ok := m[key]; ok {
		return v
	}

	return ""
}

// Set sets the metadata key to value.
func (m Metadata) Set(key, value string) {
	m[key] = value
}
