package cmd

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"

	"github.com/pkg/errors"

	"cloud.google.com/go/pubsub"
	"github.com/spf13/viper"

	"github.com/ThreeDotsLabs/watermill/message/infrastructure/googlecloud"
	"github.com/spf13/cobra"
)

var googleCloudTempSubscriptionID string

// googleCloudCmd is a mid-level command for working with the Google Cloud Pub/Sub provider.
var googleCloudCmd = &cobra.Command{
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

		if cmd.Use == "consume" {
			subName := viper.GetString("googlecloud.subscriptionName")
			if subName == "" {
				subName, err = generateTempSubscription()
				if err != nil {
					return err
				}
			}

			consumer, err = googlecloud.NewSubscriber(
				context.Background(),
				googlecloud.SubscriberConfig{
					GenerateSubscriptionName: func(topic string) string {
						return subName
					},
					ProjectID: projectID,
				},
				logger,
			)
			if err != nil {
				return err
			}
		}

		if cmd.Use == "produce" {
			producer, err = googlecloud.NewPublisher(
				context.Background(),
				googlecloud.PublisherConfig{
					ProjectID: projectID,
				},
			)

			if err != nil {
				return err
			}
		}

		return nil
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		logger.Debug("Google Cloud Pub/Sub cleanup", nil)
		if googleCloudTempSubscriptionID != "" {
			if err := removeTempSubscription(); err != nil {
				return err
			}
		}
		return nil
	},
}

func generateTempSubscription() (id string, err error) {
	ctx := context.Background()
	defer func() {
		if err == nil {
			logger.Debug("Temp subscription created", watermill.LogFields{
				"subscription_name": id,
			})
			googleCloudTempSubscriptionID = id
		}
	}()
	projectID := viper.GetString("googlecloud.projectID")

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return "", errors.Wrap(err, "could not create pubsub client")
	}

	topic := client.Topic(viper.GetString("googlecloud.consume.topic"))
	exists, err := topic.Exists(ctx)
	if err != nil {
		return "", errors.Wrap(err, "could not check if topic exists")
	}
	if !exists {
		topic, err = client.CreateTopic(ctx, topic.ID())
		if err != nil {
			return "", errors.Wrap(err, "could not create topic")
		}
	}

	randomID := "watermill_console_consumer_" + watermill.NewShortUUID()
	sub, err := client.CreateSubscription(ctx, randomID, pubsub.SubscriptionConfig{
		Topic:               topic,
		AckDeadline:         10 * time.Second,
		RetainAckedMessages: false,
	})
	if err != nil {
		return "", errors.Wrap(err, "could not create temp subscription")
	}

	return sub.ID(), nil
}

func removeTempSubscription() (err error) {
	defer func() {
		if err == nil {
			logger.Debug("Temporary subscription removed", watermill.LogFields{
				"subscription_name": googleCloudTempSubscriptionID,
			})
		}
	}()

	ctx := context.Background()
	projectID := viper.GetString("googlecloud.projectID")

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return errors.Wrap(err, "could not create pubsub client")
	}

	sub := client.Subscription(googleCloudTempSubscriptionID)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return errors.Wrap(err, "could not check if sub exists")
	}

	if !exists {
		return nil
	}

	return sub.Delete(ctx)
}

func init() {
	// Here you will define your flags and configuration settings.
	rootCmd.AddCommand(googleCloudCmd)
	consumeCmd := addConsumeCmd(googleCloudCmd)
	addProduceCmd(googleCloudCmd)

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	googleCloudCmd.PersistentFlags().String("googlecloud.projectID", "", "The projectID for Google Cloud Pub/Sub")
	if err := viper.BindPFlag("googlecloud.projectID", googleCloudCmd.PersistentFlags().Lookup("googlecloud.projectID")); err != nil {
		panic(err)
	}

	consumeCmd.PersistentFlags().String(
		"googlecloud.subscriptionName",
		"",
		"The subscription for Google Cloud Pub/Sub. If left empty, a temporary subscription is created and removed when the consumer is closed",
	)
	if err := viper.BindPFlag("googlecloud.subscriptionName", consumeCmd.PersistentFlags().Lookup("googlecloud.subscriptionName")); err != nil {
		panic(err)
	}

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
