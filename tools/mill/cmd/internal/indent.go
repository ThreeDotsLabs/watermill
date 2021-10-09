package internal

import "strings"

func Indent(s, prefix string) string {
	endsWithNewline := strings.HasSuffix(s, "\n")
	split := strings.Split(s, "\n")

	for i, ss := range split {
		split[i] = prefix + ss
	}
	joined := strings.Join(split, "\n")
	if endsWithNewline {
		joined += "\n"
	}

	return joined
}
