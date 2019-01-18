package publisher

import (
	"strings"

	"github.com/ThreeDotsLabs/watermill/message"
)

type ErrCouldNotPublish struct {
	reasons map[string]error
}

func (e *ErrCouldNotPublish) addMsg(msg *message.Message, reason error) {
	e.reasons[msg.UUID] = reason
}

func NewErrCouldNotPublish() *ErrCouldNotPublish {
	return &ErrCouldNotPublish{make(map[string]error)}
}

func (e ErrCouldNotPublish) Len() int {
	return len(e.reasons)
}

func (e ErrCouldNotPublish) Error() string {
	if len(e.reasons) == 0 {
		return ""
	}
	b := strings.Builder{}
	b.WriteString("Could not publish the messages:\n")
	for uuid, reason := range e.reasons {
		b.WriteString(uuid + " : " + reason.Error() + "\n")
	}
	return b.String()
}

func (e ErrCouldNotPublish) Reasons() map[string]error {
	return e.reasons
}
