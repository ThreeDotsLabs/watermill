package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type MessagesUpdated struct {
	Messages []Message
}

func FetchMessages(sub chan MessagesUpdated) tea.Cmd {
	db, err := sqlx.Connect("postgres", "postgres://watermill:password@localhost/watermill?sslmode=disable")
	if err != nil {
		panic(err)
	}

	repo := NewRepository(db)

	return func() tea.Msg {
		for {
			msgs, err := repo.AllMessages()
			if err != nil {
				panic(err)
			}

			sub <- MessagesUpdated{
				Messages: msgs,
			}

			time.Sleep(time.Second)
		}
	}
}

func WaitForMessages(sub chan MessagesUpdated) tea.Cmd {
	return func() tea.Msg {
		return <-sub
	}
}

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

type model struct {
	sub           chan MessagesUpdated
	chosenMessage *int
	table         table.Model
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		FetchMessages(m.sub),
		WaitForMessages(m.sub),
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if m.chosenMessage == nil {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "ctrl+c", "q", "esc":
				return m, tea.Quit
			case " ", "enter":
				c := m.table.Cursor()
				m.chosenMessage = &c
			}

		case MessagesUpdated:
			rows := make([]table.Row, len(msg.Messages))
			for i, message := range msg.Messages {
				rows[i] = table.Row{
					fmt.Sprint(message.Offset),
					message.UUID,
					message.Topic,
					//message.DelayedUntil,
					message.DelayedFor,
					message.RequeueIn.String(),
				}
			}
			m.table.SetRows(rows)
			return m, WaitForMessages(m.sub)
		}

		var cmd tea.Cmd
		m.table, cmd = m.table.Update(msg)
		return m, cmd
	} else {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "ctrl+c", "q":
				return m, tea.Quit
			case "esc":
				m.chosenMessage = nil
			}
		}

		return m, nil
	}
}

func (m model) View() string {
	if m.chosenMessage == nil {
		return baseStyle.Render(m.table.View()) + "\n  " + m.table.HelpView() + "\n"
	} else {
		row := m.table.Rows()[*m.chosenMessage]

		return row[0]
	}
}

func newModel() model {
	columns := []table.Column{
		{Title: "Offset", Width: 8},
		{Title: "UUID", Width: 40},
		//{Title: "Delayed Until", Width: 22},
		{Title: "Topic", Width: 14},
		{Title: "Delayed For", Width: 14},
		{Title: "Requeue In", Width: 14},
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithFocused(true),
		table.WithHeight(7),
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
		sub:   make(chan MessagesUpdated),
		table: t,
	}
}

func main() {
	m := newModel()

	p := tea.NewProgram(m)
	if _, err := p.Run(); err != nil {
		fmt.Printf("Alas, there's been an error: %v", err)
		os.Exit(1)
	}
}

type Message struct {
	Offset       int    `db:"offset"`
	UUID         string `db:"uuid"`
	Payload      string `db:"payload"`
	Metadata     string `db:"metadata"`
	DelayedUntil string
	DelayedFor   string
	RequeueIn    time.Duration
	Topic        string
	CreatedAt    time.Time `db:"created_at"`
}

type Repository struct {
	db *sqlx.DB
}

func NewRepository(db *sqlx.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) AllMessages() ([]Message, error) {
	var messages []Message
	err := r.db.Select(&messages, `SELECT "offset", uuid, payload, metadata, created_at FROM watermill_poison`)
	if err != nil {
		return nil, err
	}

	for i, msg := range messages {
		var metadata map[string]string
		err := json.Unmarshal([]byte(msg.Metadata), &metadata)
		if err != nil {
			return nil, err
		}
		messages[i].DelayedUntil = metadata["delayed_until"]
		messages[i].DelayedFor = metadata["delayed_for"]
		messages[i].Topic = metadata[middleware.PoisonedTopicKey]

		// Calculate the time until the message should be requeued
		delayedUntil, err := time.Parse(time.RFC3339, metadata["delayed_until"])
		if err != nil {
			return nil, err
		}

		messages[i].RequeueIn = time.Until(delayedUntil).Round(time.Second) + 2*time.Hour + 18*time.Minute
	}

	return messages, nil
}
