package mysql

import "github.com/pkg/errors"

type PublisherConfig struct {
	Database string
	Table    string

	//Marshaler MarshalMessageFunc
}

func (c PublisherConfig) validate() error {
	if c.Database == "" {
		return errors.New("database not set")
	}

	if c.Table == "" {
		return errors.New("table not set")
	}
}

type Publisher struct {
	config PublisherConfig
}

func NewPublisher(conf PublisherConfig) (*Publisher, error) {

}
