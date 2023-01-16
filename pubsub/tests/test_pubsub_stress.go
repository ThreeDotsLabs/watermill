//go:build stress
// +build stress

package tests

import (
	"runtime"
)

func init() {
	// stress tests may work a bit slower
	defaultTimeout *= 5

	// Set GOMAXPROCS to double the number of CPUs
	runtime.GOMAXPROCS(runtime.GOMAXPROCS(0) * 2)
}
