package message

type Messages []*Message

func (m Messages) IDs() []string {
	ids := make([]string, len(m))

	for i, msg := range m {
		ids[i] = msg.UUID
	}

	return ids
}
