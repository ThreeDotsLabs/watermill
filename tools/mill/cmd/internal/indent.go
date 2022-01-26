package internal

import "strings"

// Indent indents all lines in the given string with a given prefix.
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
