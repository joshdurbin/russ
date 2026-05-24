package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "russ",
	Short: "Redis Upgrade Sentinel Simulator",
	Long: `russ manages Redis and Redis Sentinel instances via Docker for testing
upgrade paths from Redis 6.x to 8.x.

All containers are created on a shared Docker bridge network named "russ".
State is tracked via Docker labels; upgrade lifecycle state is persisted to
~/.russ/state/<cluster>.json.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(func() {
		viper.SetEnvPrefix("RUSS")
		viper.AutomaticEnv()
	})

	rootCmd.AddCommand(sentinelCmd)
	rootCmd.AddCommand(clusterCmd)
	rootCmd.AddCommand(instanceCmd)
	rootCmd.AddCommand(clientCmd)
	rootCmd.AddCommand(obsCmd)
}
