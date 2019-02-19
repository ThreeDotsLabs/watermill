package cmd

import (
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/io"
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/spf13/cobra"
)

// producer is initialized by parent command to the pub/sub provider of choice.
var producer message.Publisher

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce messages to a pub/sub from the stdin",
	Long: `Produce messages to the pub/sub of your choice from the standard input.

For the configuration of particular pub/sub providers, see the help for the provider commands.`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		topic := viper.GetString("produce.topic")

		router, err := message.NewRouter(
			message.RouterConfig{
				CloseTimeout: 10 * time.Second,
			},
			logger,
		)
		if err != nil {
			return errors.Wrap(err, "could not create router")
		}

		router.AddMiddleware(middleware.InstantAck)
		router.AddPlugin(plugin.SignalsHandler)

		//in, err := io.NewSubscriber(os.Stdin, io.SubscriberConfig{
		f, err := os.Open("/tmp/stdin")
		if err != nil {
			panic(err)
		}
		in, err := io.NewSubscriber(f, io.SubscriberConfig{
			PollInterval:  time.Second,
			UnmarshalFunc: io.PayloadUnmarshalFunc,
			Logger:        logger,
		})
		if err != nil {
			return errors.Wrap(err, "could not create console subscriber")
		}

		router.AddHandler(
			"produce_from_stdin",
			"",
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

		return router.Run()
	},
}

func init() {
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	produceCmd.Flags().StringP("topic", "t", "", "The topic to produce messages to (required)")
	err := produceCmd.MarkFlagRequired("topic")
	if err != nil {
		panic(err)
	}
	if err = viper.BindPFlag("produce.topic", produceCmd.Flags().Lookup("topic")); err != nil {
		panic(err)
	}

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
