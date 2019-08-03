// +build stress

package infrastructure

func init() {
	// stress tests may work a bit slower
	defaultTimeout *= 5
}
