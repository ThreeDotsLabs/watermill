package cmd

import (
	"context"

	"github.com/spf13/viper"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"
	"github.com/spf13/cobra"
)

// googleCloudCmd is a mid-level command for working with the Google Cloud Pub/Sub provider.
var googleCloudCmd *cobra.Command = &cobra.Command{
	Use:   "googlecloud",
	Short: "Commands for the Google Cloud Pub/Sub provider",
	Long: `Consume or produce messages from the Google Cloud Pub/Sub provider. Manage subscriptions.

For the configuration of consuming/producing of the messages, check the help of the relevant command.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		err := rootCmd.PersistentPreRunE(cmd, args)
		if err != nil {
			return err
		}
		logger.Debug("Using Google Cloud Pub/Sub", nil)

		projectID := viper.GetString("googlecloud.projectID")

		producer, err = googlecloud.NewPublisher(
			context.Background(),
			googlecloud.PublisherConfig{
				ProjectID: projectID,
			},
		)

		if err != nil {
			return err
		}

		consumer, err = googlecloud.NewSubscriber(
			context.Background(),
			googlecloud.SubscriberConfig{
				ProjectID: projectID,
			},
			logger,
		)
		if err != nil {
			return err
		}

		return nil
	},
}

func init() {
	// Here you will define your flags and configuration settings.
	rootCmd.AddCommand(googleCloudCmd)
	addConsumeCmd(googleCloudCmd)
	addProduceCmd(googleCloudCmd)

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	googleCloudCmd.PersistentFlags().String("googlecloud.projectID", "", "The projectID for Google Cloud Pub/Sub")
	if err := viper.BindPFlag("googlecloud.projectID", googleCloudCmd.PersistentFlags().Lookup("googlecloud.projectID")); err != nil {
		panic(err)
	}

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
