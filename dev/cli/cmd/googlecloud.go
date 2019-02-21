package cmd

import (
	"context"
	"os"
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
					ProjectID: projectID(),
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
					ProjectID: projectID(),
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

var googleCloudSubscriptionCmd = &cobra.Command{
	Use:   "subscription",
	Short: "Manage Google Cloud Pub/Sub subscriptions",
	Long:  `Add or remove subscriptions for the Google Cloud Pub/Sub provider.`,
}

var googleCloudSubscriptionAddCmd = &cobra.Command{
	Use:       "add <subscription_id>",
	Short:     "Add a new subscription in Google Cloud Pub/Sub",
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"subscriptionID"},
	RunE: func(cmd *cobra.Command, args []string) error {
		subID := args[0]

		logger := logger.With(watermill.LogFields{
			"subscription_id": subID,
		})
		logger.Info("Creating new subscription", nil)

		return nil
	},
}

var googleCloudSubscriptionRmCmd = &cobra.Command{
	Use:       "rm <subscription_id>",
	Short:     "Remove a subscription in Google Cloud Pub/Sub",
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"subscriptionID"},
	RunE: func(cmd *cobra.Command, args []string) error {
		subID := args[0]

		logger := logger.With(watermill.LogFields{
			"subscription_id": subID,
		})
		logger.Info("Removing a subscription", nil)

		return removeSubscription(subID)
	},
}

func generateTempSubscription() (id string, err error) {
	defer func() {
		if err == nil {
			logger.Debug("Temp subscription created", watermill.LogFields{
				"subscription_name": id,
			})
			googleCloudTempSubscriptionID = id
		}
	}()

	randomID := "watermill_console_consumer_" + watermill.NewShortUUID()
	return randomID, generateSubscription(
		randomID,
		viper.GetString("googlecloud.consume.topic"),
		10*time.Second,
		false,
		time.Minute,
		nil,
	)
}

func generateSubscription(
	id string,
	topic string,
	ackDeadline time.Duration,
	retainAckedMessages bool,
	retentionDuration time.Duration,
	labels map[string]string,
) error {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID())
	if err != nil {
		return errors.Wrap(err, "could not create pubsub client")
	}

	t := client.Topic(topic)
	exists, err := t.Exists(ctx)
	if err != nil {
		return errors.Wrap(err, "could not check if topic exists")
	}
	if !exists {
		t, err = client.CreateTopic(ctx, t.ID())
		if err != nil {
			return errors.Wrap(err, "could not create topic")
		}
	}

	_, err = client.CreateSubscription(ctx, id, pubsub.SubscriptionConfig{
		Topic:               t,
		AckDeadline:         ackDeadline,
		RetainAckedMessages: retainAckedMessages,
		RetentionDuration:   retentionDuration,
		Labels:              labels,
	})
	if err != nil {
		return errors.Wrap(err, "could not create subscription")
	}

	return nil
}

func removeTempSubscription() (err error) {
	defer func() {
		if err == nil {
			logger.Debug("Temporary subscription removed", watermill.LogFields{
				"subscription_name": googleCloudTempSubscriptionID,
			})
		}
	}()
	return removeSubscription(googleCloudTempSubscriptionID)
}

func removeSubscription(id string) error {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID())
	if err != nil {
		return errors.Wrap(err, "could not create pubsub client")
	}

	sub := client.Subscription(id)
	exists, err := sub.Exists(ctx)
	if err != nil {
		return errors.Wrap(err, "could not check if sub exists")
	}

	if !exists {
		return nil
	}

	return sub.Delete(ctx)
}

func projectID() string {
	projectID := viper.GetString("googlecloud.projectID")
	if projectID == "" {
		projectID = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}

	return projectID
}

func init() {
	// Here you will define your flags and configuration settings.
	rootCmd.AddCommand(googleCloudCmd)
	consumeCmd := addConsumeCmd(googleCloudCmd, true)
	addProduceCmd(googleCloudCmd, true)

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	googleCloudCmd.PersistentFlags().String("project", "", "The projectID for Google Cloud Pub/Sub. Defaults to the GOOGLE_CLOUD_PROJECT environment variable.")
	if err := viper.BindPFlag("googlecloud.projectID", googleCloudCmd.PersistentFlags().Lookup("project")); err != nil {
		panic(err)
	}

	consumeCmd.PersistentFlags().StringP(
		"subscription",
		"s",
		"",
		"The subscription for Google Cloud Pub/Sub. If left empty, a temporary subscription is created and removed when the consumer is closed",
	)
	if err := viper.BindPFlag("googlecloud.subscriptionName", consumeCmd.PersistentFlags().Lookup("subscription")); err != nil {
		panic(err)
	}

	googleCloudCmd.AddCommand(googleCloudSubscriptionCmd)
	googleCloudSubscriptionCmd.AddCommand(googleCloudSubscriptionAddCmd)
	googleCloudSubscriptionCmd.AddCommand(googleCloudSubscriptionRmCmd)

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
