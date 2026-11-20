package cmd

import (
	"context"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
)

// producer is initialized by parent command to the pub/sub provider of choice.
var producer message.Publisher

func addProduceCmd(parent *cobra.Command, topicKey string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "produce",
		Short: "Produce messages to a pub/sub from the stdin",
		Long: `Produce messages to the pub/sub of your choice from the standard input.

For the configuration of particular pub/sub providers, see the help for the provider commands.`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := viper.GetString(topicKey)
			router, err := message.NewRouter(
				message.RouterConfig{
					CloseTimeout: 10 * time.Second,
				},
				logger,
			)
			if err != nil {
				return errors.Wrap(err, "could not create router")
			}

			router.AddPlugin(plugin.SignalsHandler)

			in := newLineSubscriber(os.Stdin)

			router.AddHandler(
				"produce_from_stdin",
				"stdin",
				in,
				topic,
				producer,
				func(msg *message.Message) ([]*message.Message, error) {
					if string(msg.Payload) == "\n" {
						logger.Trace("Message is empty, don't publish", nil)
						return nil, nil
					}
					// just pass the message along
					return message.Messages{msg}, nil
				},
			)

			if err := router.Run(context.Background()); err != nil {
				return err
			}

			// The router stops without an error when stdin ends, so a read failure has to be checked here.
			return in.Err()
		},
	}

	parent.AddCommand(cmd)
	return cmd
}
