package message

type Metadata map[string]string

func (m Metadata) Get(key string) string {
	if v, ok := m[key]; ok {
		return v
	}

	return ""
}

func (m Metadata) Set(key, value string) {
	m[key] = value
}
