package cmd

import (
	"github.com/spf13/cobra"
)

// kafkaCmd is a mid-level command for working with the kafka pub/sub provider.
var kafkaCmd = &cobra.Command{
	Use:   "kafka",
	Short: "Consume or produce messages from the kafka pub/sub provider",
	Long: `Consume or produce messages from the kafka pub/sub provider.

For the configuration of consuming/producing of the message, check the help of the relevant command.`,
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	// Here you will define your flags and configuration settings.

	rootCmd.AddCommand(kafkaCmd)
	kafkaCmd.AddCommand(consumeCmd)
	kafkaCmd.AddCommand(produceCmd)

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// produceCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
