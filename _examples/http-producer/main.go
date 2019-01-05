package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	uuid "github.com/satori/go.uuid"

	"github.com/ThreeDotsLabs/watermill/message"
	watermill_http "github.com/ThreeDotsLabs/watermill/message/infrastructure/http"
)

type id string

func (i id) String() string {
	return string(i)
}

type api struct {
	publisher message.Publisher
}

type meeting struct {
	id             id
	date           time.Time
	invitedUsers   []id
	invitationText string

	arranged bool
}

func (a api) sendInvitation(meeting id, user id, text string) error {
	payload := struct {
		MeetingID string `json:"meeting_id"`
		UserID    string `json:"user_id"`
		Text      string `json:"invitation_text"`
	}{
		meeting.String(), user.String(), text,
	}

	j, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	msg := message.NewMessage(uuid.NewV4().String(), j)

	return a.publisher.Publish("invitations", msg)
}

func (a api) bookMeeting(meetingID id, date time.Time) error {
	payload := struct {
		MeetingID string `json:"meeting_id"`
		Date      string `json:"date"`
	}{
		meetingID.String(), date.String(),
	}

	j, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	msg := message.NewMessage(uuid.NewV4().String(), j)

	return a.publisher.Publish("meetings", msg)
}

func (a api) arrange(m meeting) error {
	if m.arranged {
		return nil
	}

	err := a.bookMeeting(m.id, m.date)
	if err != nil {
		return err
	}

	for _, invitee := range m.invitedUsers {
		err = a.sendInvitation(m.id, invitee, m.invitationText)
		if err != nil {
			return err
		}
	}

	m.arranged = true
	return nil
}

func testServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fmt.Printf(
			"[%s] %s %s\n%s\n",
			time.Now().String(),
			r.Method,
			r.URL.String(),
			string(body),
		)
	}))
}

func main() {
	ts := testServer()
	defer ts.Close()

	marshallMessage := func(topic string, msg *message.Message) (*http.Request, error) {
		return http.NewRequest(http.MethodPost, ts.URL+"/"+topic, bytes.NewBuffer(msg.Payload))
	}
	logger := watermill.NopLogger{}

	publisher, err := watermill_http.NewPublisher(marshallMessage, logger)
	if err != nil {
		panic(err)
	}

	meetingsAPI := api{publisher}

	newMeeting := meeting{
		id:             id("meeting_1"),
		date:           time.Date(2019, 1, 1, 13, 0, 0, 0, time.Local),
		invitedUsers:   []id{"alice", "bob", "martha"},
		invitationText: "You are invited to my meeting.",
	}

	err = meetingsAPI.arrange(newMeeting)
	if err != nil {
		panic(err)
	}
}
