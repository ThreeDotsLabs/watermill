package cmd

import (
	"context"
	"os"
	"strings"
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
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		subID := args[0]

		topic := viper.GetString("googlecloud.subscription.add.topic")
		ackDeadline := viper.GetDuration("googlecloud.subscription.add.ackDeadline")
		retainAcked := viper.GetBool("googlecloud.subscription.add.retainAcked")
		retentionDuration := viper.GetDuration("googlecloud.subscription.add.retentionDuration")

		// StringToString doesn't work with viper, so let's parse this manually
		labels := strings.Split(viper.GetString("googlecloud.subscription.add.labels"), ",")
		labelsMap := make(map[string]string, len(labels))
		for _, l := range labels {
			fields := strings.Split(l, "=")
			if len(fields) < 2 {
				continue
			}
			labelsMap[fields[0]] = fields[1]
		}

		logger := logger.With(watermill.LogFields{
			"subscription_id":   subID,
			"topic":             topic,
			"ackDeadline":       ackDeadline,
			"retainAcked":       retainAcked,
			"retentionDuration": retentionDuration,
			"labels":            labelsMap,
		})
		logger.Info("Creating new subscription", nil)

		defer func() {
			if err == nil {
				logger.Info("Subscription created", nil)
			}
		}()

		return addSubscription(
			subID,
			topic,
			ackDeadline,
			retainAcked,
			retentionDuration,
			labelsMap,
		)
	},
}

var googleCloudSubscriptionRmCmd = &cobra.Command{
	Use:       "rm <subscription_id>",
	Short:     "Remove a subscription in Google Cloud Pub/Sub",
	Args:      cobra.ExactArgs(1),
	ValidArgs: []string{"subscriptionID"},
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		subID := args[0]

		logger := logger.With(watermill.LogFields{
			"subscription_id": subID,
		})
		logger.Info("Removing a subscription", nil)

		defer func() {
			if err == nil {
				logger.Info("Subscription removed", nil)
			}
		}()

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
	return randomID, addSubscription(
		randomID,
		viper.GetString("googlecloud.consume.topic"),
		10*time.Second,
		false,
		time.Minute,
		nil,
	)
}

func addSubscription(
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

	googleCloudSubscriptionAddCmd.Flags().StringP("topic", "t", "", "The topic for the new subscription (required)")
	err := googleCloudSubscriptionAddCmd.MarkFlagRequired("topic")
	if err != nil {
		panic(err)
	}
	if err = viper.BindPFlag("googlecloud.subscription.add.topic", googleCloudSubscriptionAddCmd.Flags().Lookup("topic")); err != nil {
		panic(err)
	}

	googleCloudSubscriptionAddCmd.Flags().DurationP(
		"ackDeadline",
		"a",
		10*time.Second,
		"How long Pub/Sub waits for the subscriber to acknowledge receipt before resending the message. Deadline time is from 10 seconds to 600 seconds",
	)
	if err = viper.BindPFlag("googlecloud.subscription.add.ackDeadline", googleCloudSubscriptionAddCmd.Flags().Lookup("ackDeadline")); err != nil {
		panic(err)
	}

	googleCloudSubscriptionAddCmd.Flags().Bool(
		"retainAcked",
		false,
		"Acknowledged messages will be kept 7 days from publication unless set otherwise in \"message retention duration\".",
	)
	if err = viper.BindPFlag("googlecloud.subscription.add.retainAcked", googleCloudSubscriptionAddCmd.Flags().Lookup("retainAcked")); err != nil {
		panic(err)
	}

	googleCloudSubscriptionAddCmd.Flags().Duration(
		"retentionDuration",
		7*24*time.Hour,
		"How long the retained messages will be kept. The allowed duration is from 10 minutes to 7 days, which is the default.",
	)
	if err = viper.BindPFlag("googlecloud.subscription.add.retentionDuration", googleCloudSubscriptionAddCmd.Flags().Lookup("retentionDuration")); err != nil {
		panic(err)
	}

	// StringToString doesn't work correctly with viper
	googleCloudSubscriptionAddCmd.Flags().String(
		"labels",
		"",
		"The set of labels for the subscription. Format: '--labels key1=value1,key2=value2,...'",
	)
	if err = viper.BindPFlag("googlecloud.subscription.add.labels", googleCloudSubscriptionAddCmd.Flags().Lookup("labels")); err != nil {
		panic(err)
	}

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
