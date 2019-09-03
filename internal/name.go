package internal

import (
	"fmt"
	"strings"
)

func StructName(v interface{}) string {
	if s, ok := v.(fmt.Stringer); ok {
		return s.String()
	}

	s := fmt.Sprintf("%T", v)
	// trim the pointer marker, if any
	return strings.TrimLeft(s, "*")
}
