package main

import (
	"context"
	"flag"
	"log"

	"github.com/ThreeDotsLabs/watermill/tools/pq/backend"
	"github.com/ThreeDotsLabs/watermill/tools/pq/cli"

	tea "github.com/charmbracelet/bubbletea"
)

var (
	backendFlag  = flag.String("backend", "", "backend to use")
	topicFlag    = flag.String("topic", "", "topic to use")
	rawTopicFlag = flag.String("raw-topic", "", "raw topic to use")
)

func main() {
	flag.Parse()

	config := cli.BackendConfig{
		Topic:    *topicFlag,
		RawTopic: *rawTopicFlag,
	}

	err := config.Validate()
	if err != nil {
		log.Fatal(err)
	}

	var b cli.Backend
	switch *backendFlag {
	case "postgres":
		b, err = backend.NewPostgresBackend(context.Background(), config)
		if err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown backend: %s", *backendFlag)
	}

	m := cli.NewModel(b)

	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	if err != nil {
		log.Fatal(err)
	}
}
