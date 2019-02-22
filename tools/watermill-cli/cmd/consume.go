package cmd

import (
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/io"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/spf13/cobra"
)

// consumer is initialized by the parent command to the pub/sub provider of choice.
var consumer message.Subscriber

func addConsumeCmd(parent *cobra.Command, topicKey string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consume",
		Short: "Consume messages from a pub/sub and print them to stdout",
		Long: `Consume messages from the pub/sub of your choice and print them on the standard output.

For the configuration of particular pub/sub providers, see the help for the provider commands.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := viper.GetString(topicKey)
			router, err := message.NewRouter(
				message.RouterConfig{
					CloseTimeout: 5 * time.Second,
				},
				logger,
			)
			if err != nil {
				return errors.Wrap(err, "could not create router")
			}

			router.AddMiddleware(middleware.InstantAck)
			router.AddPlugin(plugin.SignalsHandler)

			out, err := io.NewPublisher(os.Stdout, io.PublisherConfig{
				MarshalFunc: io.PrettyPayloadMarshalFunc,
			})
			if err != nil {
				return errors.Wrap(err, "could not create console producer")
			}

			router.AddHandler(
				"dump_to_stdout",
				topic,
				consumer,
				"",
				out,
				func(msg *message.Message) ([]*message.Message, error) {
					// just forward the message to stdout
					return message.Messages{msg}, nil
				},
			)

			return router.Run()
		},
	}

	parent.AddCommand(cmd)
	return cmd
}