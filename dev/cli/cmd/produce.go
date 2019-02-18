package cmd

import (
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
	Run: func(cmd *cobra.Command, args []string) {
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

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
