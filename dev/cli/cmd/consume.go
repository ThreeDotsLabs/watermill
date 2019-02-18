package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// consumeCmd represents the consume command
var consumeCmd = &cobra.Command{
	Use:   "consume",
	Short: "Consume messages from a pub/sub and print them to stdout",
	Long: `Consume messages from the pub/sub of your choice and print them on the standard output.

For the configuration of particular pub/sub providers, see the help for the provider commands.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("consume called")
	},
}

func init() {
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// consumeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// consumeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
