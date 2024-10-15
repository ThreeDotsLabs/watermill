package main

import (
	"context"
	"log"

	"github.com/ThreeDotsLabs/watermill/tools/pq/backend"
	"github.com/ThreeDotsLabs/watermill/tools/pq/cli"

	tea "github.com/charmbracelet/bubbletea"
)

func main() {
	config := cli.BackendConfig{
		Topic:    "poison",
		RawTopic: "",
	}

	err := config.Validate()
	if err != nil {
		log.Fatal(err)
	}

	b, err := backend.NewPostgresBackend(context.Background(), config)
	if err != nil {
		log.Fatal(err)
	}

	m := cli.NewModel(b)

	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	if err != nil {
		log.Fatal(err)
	}
}
