package sql

import (
	"fmt"
	"strings"
)

// sqlArgsToLog is used for "lazy" generating sql args strings to logger
type sqlArgsToLog []interface{}

func (s sqlArgsToLog) String() string {
	var strArgs []string
	for _, arg := range s {
		strArgs = append(strArgs, fmt.Sprintf("%s", arg))
	}

	return strings.Join(strArgs, ",")
}

func isDeadlock(err error) bool {
	// ugly, but should be universal for multiple sql implementations
	if strings.Contains(strings.ToLower(err.Error()), "deadlock") {
		return true
	}

	return false
}
