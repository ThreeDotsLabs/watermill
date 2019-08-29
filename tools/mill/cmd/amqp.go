package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
)

var amqpCmd = &cobra.Command{
	Use:   "amqp",
	Short: "Commands for the AMQP Pub/Sub provider",
	Long: `Consume or produce messages from the AMQP Pub/Sub provider.

For the configuration of consuming/producing of the messages, check the help of the relevant command.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		err := rootCmd.PersistentPreRunE(cmd, args)
		if err != nil {
			return err
		}

		logger.Debug("Using AMQP Pub/Sub", nil)

		if cmd.Use == "consume" {
			consumer, err = amqp.NewSubscriber(amqpConsumerConfig(), logger)
			if err != nil {
				return err
			}
		}

		if cmd.Use == "produce" {
			producer, err = amqp.NewPublisher(amqpProducerConfig(), logger)
			if err != nil {
				return err
			}
		}

		return nil
	},
}

func amqpConsumerConfig() amqp.Config {
	uri := viper.GetString("amqp.uri")
	queue := viper.GetString("amqp.consume.queue")
	exchangeName := viper.GetString("amqp.consume.exchange")
	exchangeType := viper.GetString("amqp.produce.exchangeType")
	durable := viper.GetBool("amqp.durable")

	return amqp.Config{
		Connection: amqp.ConnectionConfig{
			AmqpURI: uri,
		},
		Marshaler: amqp.DefaultMarshaler{},
		Queue: amqp.QueueConfig{
			GenerateName: func(topic string) string {
				return queue
			},
			Durable: durable,
		},
		Consume: amqp.ConsumeConfig{
			Qos: amqp.QosConfig{
				PrefetchCount: 1,
			},
		},

		Exchange: amqp.ExchangeConfig{
			GenerateName: func(topic string) string {
				return exchangeName
			},
			Type:    exchangeType,
			Durable: durable,
		},
	}
}

func amqpProducerConfig() amqp.Config {
	uri := viper.GetString("amqp.uri")
	exchangeName := viper.GetString("amqp.produce.exchange")
	exchangeType := viper.GetString("amqp.produce.exchangeType")
	routingKey := viper.GetString("amqp.produce.routingKey")
	durable := viper.GetBool("amqp.durable")

	return amqp.Config{
		Connection: amqp.ConnectionConfig{
			AmqpURI: uri,
		},
		Marshaler: amqp.DefaultMarshaler{},
		Exchange: amqp.ExchangeConfig{
			GenerateName: func(topic string) string {
				return exchangeName
			},
			Type:    exchangeType,
			Durable: durable,
		},
		Publish: amqp.PublishConfig{
			GenerateRoutingKey: func(topic string) string {
				return routingKey
			},
		},
	}
}

func init() {
	rootCmd.AddCommand(amqpCmd)
	configureAmqpCmd()
	consumeCmd := addConsumeCmd(amqpCmd, "amqp.consume.queue")
	configureConsumeCmd(consumeCmd)
	produceCmd := addProduceCmd(amqpCmd, "amqp.produce.exchange")
	configureProduceCmd(produceCmd)
}

func configureAmqpCmd() {
	amqpCmd.PersistentFlags().StringP(
		"uri",
		"u",
		"",
		"The URI to the AMQP instance (required)",
	)
	ensure(amqpCmd.MarkPersistentFlagRequired("uri"))
	ensure(viper.BindPFlag("amqp.uri", amqpCmd.PersistentFlags().Lookup("uri")))

	amqpCmd.PersistentFlags().Bool(
		"durable",
		true,
		"If true, the queues and exchanges created automatically (if any) will be durable",
	)
	ensure(viper.BindPFlag("amqp.durable", amqpCmd.PersistentFlags().Lookup("durable")))

	amqpCmd.PersistentFlags().String(
		"exchange-type",
		"fanout",
		"If exchange needs to be created, it will be created with this type. The common types are 'direct', 'fanout', 'topic' and 'headers'.",
	)
	ensure(viper.BindPFlag("amqp.produce.exchangeType", amqpCmd.PersistentFlags().Lookup("exchange-type")))
}

func configureConsumeCmd(consumeCmd *cobra.Command) {
	consumeCmd.PersistentFlags().StringP(
		"queue",
		"q",
		"",
		"The name of the AMQP queue to consume messages from (required)",
	)
	ensure(consumeCmd.MarkPersistentFlagRequired("queue"))
	ensure(viper.BindPFlag("amqp.consume.queue", consumeCmd.PersistentFlags().Lookup("queue")))

	consumeCmd.PersistentFlags().StringP(
		"exchange",
		"x",
		"",
		"If non-empty, an exchange with this name is created if it didn't exist. Then, the queue is bound to this exchange.",
	)
	ensure(viper.BindPFlag("amqp.consume.exchange", consumeCmd.PersistentFlags().Lookup("exchange")))
}

func configureProduceCmd(produceCmd *cobra.Command) {
	produceCmd.PersistentFlags().StringP(
		"exchange",
		"x",
		"",
		"The name of the AMQP exchange to produce messages to (required)",
	)
	ensure(produceCmd.MarkPersistentFlagRequired("exchange"))
	ensure(viper.BindPFlag("amqp.produce.exchange", produceCmd.PersistentFlags().Lookup("exchange")))

	produceCmd.PersistentFlags().StringP(
		"routing-key",
		"r",
		"",
		"The routing key to use when publishing the message.",
	)
	ensure(produceCmd.MarkPersistentFlagRequired("routing-key"))
	ensure(viper.BindPFlag("amqp.produce.routingKey", produceCmd.PersistentFlags().Lookup("routing-key")))
}
