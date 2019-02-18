package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "Produce messages to a pub/sub from the stdin",
	Long: `Produce messages to the pub/sub of your choice from the standard input.

For the configuration of particular pub/sub providers, see the help for the provider commands.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("produce called")
	},
}

func init() {
	rootCmd.AddCommand(produceCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// produceCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// produceCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
