package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/pflag"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "watermill-cli",
	Short: "A CLI for Watermill.",
	Long: `A CLI for Watermill.

Use console-based producer or consumer for various pub/sub providers.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().SortFlags = false

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.watermill-cli.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	outputFlags := pflag.NewFlagSet("output", pflag.ExitOnError)
	outputFlags.BoolP("log", "l", false, "If true, the logger output is sent to stderr. No logger output otherwise.")
	outputFlags.Bool("debug", false, "If true, debug output is enabled from the logger")
	outputFlags.Bool("trace", false, "If true, trace output is enabled from the logger")
	rootCmd.PersistentFlags().AddFlagSet(outputFlags)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".cli" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".watermill-cli")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
