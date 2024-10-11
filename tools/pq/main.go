package main

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/delay"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	_ "github.com/lib/pq"
	"golang.org/x/exp/maps"
)

var messageActions = []string{"<- Back", "Show payload", "Requeue", "Ack (drop)"}

type MessagesUpdated struct {
	Messages []Message
}

func (m model) FetchMessages() tea.Cmd {
	return func() tea.Msg {
		for {
			msgs, err := m.repo.AllMessages(m.topic)
			if err != nil {
				panic(err)
			}

			m.sub <- MessagesUpdated{
				Messages: msgs,
			}

			time.Sleep(time.Second)
		}
	}
}

func (m model) WaitForMessages() tea.Cmd {
	return func() tea.Msg {
		return <-m.sub
	}
}

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

type model struct {
	repo  repository
	topic string
	sub   chan MessagesUpdated

	chosenMessage   *int
	chosenMessageID string

	table    table.Model
	messages []Message

	chosenAction int

	showingPayload  bool
	payloadViewport viewport.Model
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.FetchMessages(),
		m.WaitForMessages(),
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case MessagesUpdated:
		rows := make([]table.Row, len(msg.Messages))
		for i, message := range msg.Messages {
			rows[i] = table.Row{
				message.ID,
				message.UUID,
				message.OriginalTopic,
				message.DelayedFor,
				message.RequeueIn.String(),
			}
		}
		m.table.SetRows(rows)
		m.messages = msg.Messages

		// If the chosen message is no longer in the list, go back to the table.
		// This is to avoid accidentally making an action on a message that has been requeued or deleted.
		// TODO consider showing information in the view instead
		if m.chosenMessage != nil {
			if m.chosenMessageID != m.messages[*m.chosenMessage].ID {
				m.chosenMessage = nil
				m.chosenMessageID = ""
			}
		}

		return m, m.WaitForMessages()
	}

	if m.chosenMessage == nil {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "ctrl+c", "q":
				return m, tea.Quit
			case " ", "enter":
				c := m.table.Cursor()
				m.chosenAction = 0
				m.chosenMessage = &c
				m.chosenMessageID = m.messages[c].ID
			}
		}

		var cmd tea.Cmd
		m.table, cmd = m.table.Update(msg)
		return m, cmd
	} else {
		if m.showingPayload {
			switch msg := msg.(type) {
			case tea.KeyMsg:
				switch msg.String() {
				case "ctrl+c", "q":
					return m, tea.Quit
				case "esc", "backspace":
					m.showingPayload = false
				}
			}

			var cmd tea.Cmd
			m.payloadViewport, cmd = m.payloadViewport.Update(msg)
			return m, cmd
		} else {
			switch msg := msg.(type) {
			case tea.KeyMsg:
				switch msg.String() {
				case "ctrl+c", "q":
					return m, tea.Quit
				case "esc", "backspace":
					m.chosenMessage = nil
					m.chosenMessageID = ""
				case "j", "down":
					m.chosenAction++
					if m.chosenAction >= len(messageActions) {
						m.chosenAction = len(messageActions) - 1
					}
				case "k", "up":
					m.chosenAction--
					if m.chosenAction < 0 {
						m.chosenAction = 0
					}
				case " ", "enter":
					switch m.chosenAction {
					case 0:
						m.chosenMessage = nil
						m.chosenMessageID = ""
					case 1:
						// Show payload
						m.showingPayload = true
						m.payloadViewport = viewport.New(80, 20)
						b := lipgloss.RoundedBorder()
						m.payloadViewport.Style = lipgloss.NewStyle().BorderStyle(b).Padding(0, 1)

						payload := m.messages[*m.chosenMessage].Payload

						var jsonPayload any
						err := json.Unmarshal([]byte(payload), &jsonPayload)
						if err == nil {
							pretty, err := json.MarshalIndent(jsonPayload, "", "    ")
							if err == nil {
								payload = string(pretty)
							}
						}

						m.payloadViewport.SetContent(payload)
					case 2:
						// Requeue
						// TODO make a command
						chosenMsg := m.messages[*m.chosenMessage]
						err := m.repo.Requeue(m.topic, chosenMsg.ID)
						if err != nil {
							panic(err)
						}
					case 3:
						// Ack
						// TODO make a command
						chosenMsg := m.messages[*m.chosenMessage]
						err := m.repo.Ack(m.topic, chosenMsg.ID)
						if err != nil {
							panic(err)
						}
					}
				}
			}
		}

		return m, nil
	}
}

func (m model) View() string {
	if m.chosenMessage == nil {
		return baseStyle.Render(m.table.View()) + "\n  " + m.table.HelpView() + "\n"
	} else {
		msg := m.messages[*m.chosenMessage]

		out := fmt.Sprintf(
			"ID: %v\nUUID: %v\nOriginal Topic: %v\nDelayed For: %v\nDelayed Until: %v\nRequeue In: %v\n\n",
			msg.ID,
			msg.UUID,
			msg.OriginalTopic,
			msg.DelayedFor,
			msg.DelayedUntil,
			msg.RequeueIn,
		)

		if m.showingPayload {
			out += m.payloadViewport.View()
			return out
		}

		out += "Metadata:\n"

		keys := maps.Keys(msg.Metadata)
		slices.Sort(keys)
		for _, k := range keys {
			v := msg.Metadata[k]
			out += fmt.Sprintf("  %v: %v\n", k, v)
		}

		out += "\nActions:"

		for i, action := range messageActions {
			if i == m.chosenAction {
				out += fmt.Sprintf("\n  %v", lipgloss.NewStyle().Background(lipgloss.Color("57")).Render(action))
			} else {
				out += fmt.Sprintf("\n  %v", action)
			}
		}

		return out
	}
}

func newModel(repo repository, topic string) model {
	columns := []table.Column{
		{Title: "ID", Width: 8},
		{Title: "UUID", Width: 40},
		{Title: "Original Topic", Width: 20},
		{Title: "Delayed For", Width: 14},
		{Title: "Requeue In", Width: 14},
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithFocused(true),
		table.WithHeight(20),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)
	t.SetStyles(s)

	return model{
		repo:  repo,
		sub:   make(chan MessagesUpdated),
		topic: topic,
		table: t,
	}
}

func main() {
	repo, err := NewPostgresRepository("postgres://watermill:password@localhost/watermill?sslmode=disable")
	if err != nil {
		panic(err)
	}

	m := newModel(repo, "poison")

	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("error: %v", err)
		os.Exit(1)
	}
}

type Message struct {
	// ID is a unique message ID across the Pub/Sub's topic.
	ID       string
	UUID     string
	Payload  string
	Metadata map[string]string

	OriginalTopic string
	DelayedUntil  string
	DelayedFor    string
	RequeueIn     time.Duration
}

func NewMessage(id string, uuid string, payload string, metadata map[string]string) (Message, error) {
	originalTopic := metadata[middleware.PoisonedTopicKey]

	// Calculate the time until the message should be requeued
	delayedUntil, err := time.Parse(time.RFC3339, metadata[delay.DelayedUntilKey])
	if err != nil {
		return Message{}, err
	}

	delayedFor := metadata[delay.DelayedForKey]
	requeueIn := delayedUntil.Sub(time.Now().UTC()).Round(time.Second)

	return Message{
		ID:            id,
		UUID:          uuid,
		Payload:       payload,
		Metadata:      metadata,
		OriginalTopic: originalTopic,
		DelayedUntil:  delayedUntil.String(),
		DelayedFor:    delayedFor,
		RequeueIn:     requeueIn,
	}, nil
}

type repository interface {
	AllMessages(topic string) ([]Message, error)
	Requeue(topic string, id string) error
	Ack(topic string, id string) error
}
