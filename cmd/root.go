package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
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
	rootCmd.PersistentFlags().BoolP("verbose", "v", false,
		"Verbose logging: flips zerolog level to debug and emits per-waypoint activity to stderr")

	cobra.OnInitialize(func() {
		viper.SetEnvPrefix("RUSS")
		viper.AutomaticEnv()
		_ = viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))

		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
		if viper.GetBool("verbose") {
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		} else {
			zerolog.SetGlobalLevel(zerolog.InfoLevel)
		}
	})

	rootCmd.AddCommand(sentinelCmd)
	rootCmd.AddCommand(clusterCmd)
	rootCmd.AddCommand(instanceCmd)
	rootCmd.AddCommand(clientCmd)
	rootCmd.AddCommand(obsCmd)
}
