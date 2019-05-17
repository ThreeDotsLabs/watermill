package infrastructure

import "flag"

type Config interface {
	GetFlagSet(*flag.FlagSet)
	Validate() error
}
