package message

// Messages is a slice of messages.
type Messages []*Message

// IDs returns a slice of Messages' IDs.
func (m Messages) IDs() []string {
	ids := make([]string, len(m))

	for i, msg := range m {
		ids[i] = msg.UUID
	}

	return ids
}
