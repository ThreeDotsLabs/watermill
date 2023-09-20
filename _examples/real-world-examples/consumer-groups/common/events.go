package common

type UserSignedUp struct {
	UserID   string   `json:"id"`
	Consents Consents `json:"consents"`
}

type Consents struct {
	Marketing bool `json:"marketing"`
	News      bool `json:"news"`
}

type MessageReceived struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Handler string `json:"handler"`
	Topic   string `json:"topic"`
}
