package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ThreeDotsLabs/watermill-amqp/v3/pkg/amqp"
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

// Both configs start from a watermill-amqp constructor because the library dereferences
// TopologyBuilder and QueueBind.GenerateRoutingKey without checking them. The durable constructor
// is used also for --durable=false, because the non-durable one switches messages to
// non-persistent delivery, which the flag never controlled.

func amqpConsumerConfig() amqp.Config {
	uri := viper.GetString("amqp.uri")
	queue := viper.GetString("amqp.consume.queue")
	exchangeName := viper.GetString("amqp.consume.exchange")
	exchangeType := viper.GetString("amqp.produce.exchangeType")
	bindingKey := viper.GetString("amqp.consume.bindingKey")
	durable := viper.GetBool("amqp.durable")

	config := amqp.NewDurablePubSubConfig(uri, amqp.GenerateQueueNameConstant(queue))
	config.Queue.Durable = durable
	config.Exchange.GenerateName = amqp.GenerateExchangeNameConstant(exchangeName)
	config.Exchange.Type = exchangeType
	config.Exchange.Durable = durable
	config.QueueBind.GenerateRoutingKey = func(topic string) string {
		return bindingKey
	}

	return config
}

func amqpProducerConfig() amqp.Config {
	uri := viper.GetString("amqp.uri")
	exchangeName := viper.GetString("amqp.produce.exchange")
	exchangeType := viper.GetString("amqp.produce.exchangeType")
	routingKey := viper.GetString("amqp.produce.routingKey")
	durable := viper.GetBool("amqp.durable")

	config := amqp.NewDurablePubSubConfig(uri, nil)
	config.Exchange.GenerateName = amqp.GenerateExchangeNameConstant(exchangeName)
	config.Exchange.Type = exchangeType
	config.Exchange.Durable = durable
	config.Publish.GenerateRoutingKey = func(topic string) string {
		return routingKey
	}

	return config
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

	consumeCmd.PersistentFlags().String(
		"binding-key",
		"",
		"The binding key used to bind the queue to the exchange (only with --exchange). Fanout exchanges ignore it.",
	)
	ensure(viper.BindPFlag("amqp.consume.bindingKey", consumeCmd.PersistentFlags().Lookup("binding-key")))
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
