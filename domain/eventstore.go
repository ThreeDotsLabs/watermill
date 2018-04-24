package domain

// todo - rename??
type Eventstore interface {
	// todo - doc why no error?
	// todo - rename????
	// todo - it is stupid to call it every time (easy to forget)
	// todo - make it work with transaction (!!!)
	Save(events []Event) error
}
