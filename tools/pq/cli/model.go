package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"golang.org/x/exp/maps"
)

var warningStyle = lipgloss.NewStyle().
	Background(lipgloss.Color("196")).
	Align(lipgloss.Center).
	Padding(1, 10)

var dialogStyle = lipgloss.NewStyle().
	Border(lipgloss.RoundedBorder()).
	BorderForeground(lipgloss.Color("241")).
	Padding(1, 4)

var buttonStyle = lipgloss.NewStyle()

var buttonSelectedStyle = lipgloss.NewStyle().
	Background(lipgloss.Color("57"))

var readOnlyMessageActions = []string{"<- Back", "Show payload"}
var writeMessageActions = []string{"Requeue", "Ack (drop)"}

var dialogActions = []string{"Cancel", "Confirm"}

type MessagesUpdated struct {
	Messages []Message
}

type DialogResult struct {
	Err error
}

func (m Model) FetchMessages() tea.Cmd {
	return func() tea.Msg {
		for {
			msgs, err := m.backend.AllMessages(context.Background())
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

func (m Model) WaitForMessages() tea.Cmd {
	return func() tea.Msg {
		return <-m.sub
	}
}

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

type Model struct {
	backend Backend
	sub     chan MessagesUpdated

	chosenMessage     *Message
	chosenMessageGone bool

	table    table.Model
	messages []Message

	chosenAction  int
	currentDialog *Dialog

	showingPayload  bool
	payloadViewport viewport.Model
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(
		m.FetchMessages(),
		m.WaitForMessages(),
	)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
		if m.chosenMessage != nil {
			found := false
			for _, message := range m.messages {
				if message.ID == m.chosenMessage.ID {
					foundMessage := message
					m.chosenMessage = &foundMessage
					found = true
					break
				}
			}

			if found {
				m.chosenMessageGone = false
			} else {
				if !m.chosenMessageGone {
					m.chosenAction = 0
				}

				m.chosenMessageGone = true
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
				chosenMessage := m.messages[c]
				m.chosenMessage = &chosenMessage
				m.chosenMessageGone = false
			}
		}

		var cmd tea.Cmd
		m.table, cmd = m.table.Update(msg)
		return m, cmd
	} else if m.showingPayload {
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
	} else if m.currentDialog != nil {
		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "ctrl+c", "q":
				return m, tea.Quit
			case "esc", "backspace":
				m.currentDialog = nil
			case "h", "left":
				m.currentDialog.Choice--
				if m.currentDialog.Choice < 0 {
					m.currentDialog.Choice = 0
				}
			case "l", "right":
				m.currentDialog.Choice++
				if m.currentDialog.Choice >= len(dialogActions) {
					m.currentDialog.Choice = len(dialogActions) - 1
				}
			case " ", "enter":
				switch m.currentDialog.Choice {
				case 0:
					m.currentDialog = nil
				case 1:
					m.currentDialog.Running = true
					return m, m.currentDialog.Action
				}
			}
		case DialogResult:
			if msg.Err != nil {
				// TODO Could be handled better
				panic(msg.Err)
			}

			m.currentDialog = nil
		}

		return m, nil
	} else {
		messageActions := len(readOnlyMessageActions)
		if !m.chosenMessageGone {
			messageActions += len(writeMessageActions)
		}

		switch msg := msg.(type) {
		case tea.KeyMsg:
			switch msg.String() {
			case "ctrl+c", "q":
				return m, tea.Quit
			case "esc", "backspace":
				m.chosenMessage = nil
				m.chosenMessageGone = false
			case "j", "down":
				m.chosenAction++
				if m.chosenAction >= messageActions {
					m.chosenAction = messageActions - 1
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
					m.chosenMessageGone = false
				case 1:
					// Show payload
					m.showingPayload = true
					m.payloadViewport = viewport.New(80, 20)
					b := lipgloss.RoundedBorder()
					m.payloadViewport.Style = lipgloss.NewStyle().BorderStyle(b).Padding(0, 1)

					payload := m.chosenMessage.Payload

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
					chosenMessage := *m.chosenMessage
					m.currentDialog = &Dialog{
						Prompt: "Requeue message? It will go back to the original topic.",
						Action: func() tea.Msg {
							ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
							defer cancel()
							return DialogResult{
								Err: m.backend.Requeue(ctx, chosenMessage),
							}
						},
					}
				case 3:
					chosenMessage := *m.chosenMessage
					m.currentDialog = &Dialog{
						Prompt: "Acknowledge message? It will be dropped from the topic.",
						Action: func() tea.Msg {
							ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
							defer cancel()
							return DialogResult{
								Err: m.backend.Ack(ctx, chosenMessage),
							}
						},
					}
				}
			}
		}

		return m, nil
	}
}

func (m Model) View() string {
	if m.chosenMessage == nil {
		return baseStyle.Render(m.table.View()) + "\n  " + m.table.HelpView() + "\n"
	}

	msg := m.chosenMessage

	var out string

	if m.chosenMessageGone {
		out += warningStyle.Render("Read only — the message is gone.")
		out += "\n"
	}

	out += fmt.Sprintf(
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

	if m.currentDialog != nil {
		prompt := m.currentDialog.Prompt + "\n\n"

		if m.currentDialog.Running {
			prompt += "Running..."
		} else {
			for i, action := range dialogActions {
				style := buttonStyle
				if i == m.currentDialog.Choice {
					style = buttonSelectedStyle
				}

				prompt += fmt.Sprintf("%v", style.MarginLeft(13).Render(action))
			}
		}

		out += dialogStyle.Render(prompt)
	} else {
		out += "\nActions:\n"

		messageActions := readOnlyMessageActions
		if !m.chosenMessageGone {
			messageActions = append(messageActions, writeMessageActions...)
		}

		for i, action := range messageActions {
			style := buttonStyle
			if i == m.chosenAction {
				style = buttonSelectedStyle
			}

			out += fmt.Sprintf("%v\n", style.MarginLeft(2).Render(action))
		}
	}

	return out
}

func NewModel(backend Backend) Model {
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

	return Model{
		backend: backend,
		sub:     make(chan MessagesUpdated),
		table:   t,
	}
}

type Dialog struct {
	Prompt  string
	Action  func() tea.Msg
	Choice  int
	Running bool
}
