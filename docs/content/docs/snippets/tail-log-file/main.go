package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-io/pkg/io"
	"github.com/ThreeDotsLabs/watermill/message"
)

// this will `tail -f` a log file and publish an alert if a line fulfils some criterion

func main() {
	// if an alert is raised, the offending line will be publisher on this
	// this would be set to an actual publisher
	var alertPublisher message.Publisher

	if len(os.Args) < 2 {
		panic(
			fmt.Errorf("usage: %s /path/to/file.log", os.Args[0]),
		)
	}
	logFile, err := os.OpenFile(os.Args[1], os.O_RDONLY, 0444)
	if err != nil {
		panic(err)
	}

	sub, err := io.NewSubscriber(logFile, io.SubscriberConfig{
		UnmarshalFunc: io.PayloadUnmarshalFunc,
	}, watermill.NewStdLogger(true, false))
	if err != nil {
		panic(err)
	}

	// for io.Subscriber, topic does not matter
	lines, err := sub.Subscribe(context.Background(), "")
	if err != nil {
		panic(err)
	}

	for line := range lines {
		if criterion(string(line.Payload)) {
			_ = alertPublisher.Publish("alerts", line)
		}
	}
}

func criterion(line string) bool {
	// decide whether an action needs to be taken
	return false
}
