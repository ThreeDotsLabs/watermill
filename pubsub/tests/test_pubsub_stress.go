// +build stress

package tests

func init() {
	// stress tests may work a bit slower
	defaultTimeout *= 5
}
