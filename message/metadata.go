package message

import "encoding/json"

type Metadata map[string]string

type metadataItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (m Metadata) Get(key string) string {
	if v, ok := m[key]; ok {
		return v
	}

	return ""
}

func (m Metadata) Set(key, value string) {
	m[key] = value
}

func (m *Metadata) UnmarshalJSON(b []byte) error {
	if *m == nil {
		*m = Metadata{}
	}
	var items []metadataItem
	if err := json.Unmarshal(b, &items); err != nil {
		return err
	}

	for _, it := range items {
		m.Set(it.Key, it.Value)
	}
	return nil
}

func (m Metadata) MarshalJSON() ([]byte, error) {
	items := make([]metadataItem, len(m))

	i := 0
	for k, v := range m {
		items[i] = metadataItem{k, v}
		i++
	}

	return json.Marshal(items)
}
