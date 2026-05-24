// russ-client is the workload simulator that runs inside Docker against a
// russ-managed Redis Sentinel fleet. It discovers monitored masters via
// SENTINEL MASTERS, picks one by name, and runs a configurable mix of
// scenarios that exercise data structures whose behavior or availability
// differs between Redis 6.x and 8.x. Prometheus metrics are exposed on
// /metrics for the russ Prometheus instance to scrape via docker_sd.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bigcommerce/russ/internal/client"
	"github.com/bigcommerce/russ/internal/client/workload"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "russ-client",
	Short: "Workload simulator targeting a russ Sentinel-managed cluster",
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List masters discovered via sentinel and exit",
	RunE:  runList,
}

var workloadCmd = &cobra.Command{
	Use:   "workload",
	Short: "Run the writer + master reader + replica observer against the sentinel-reported cluster",
	Long: `Starts three concurrent roles against the chosen cluster:

  writer    — INCR a counter and SET a "<n>:<unix-nano>" value on the master
  reader    — GET that value back from the master, recording its age
  observer  — for each replica returned by SENTINEL REPLICAS, GET the same key
              directly (read-only via the replica) and emit lag/value metrics

All three are simple Redis 6-compatible operations. The purpose is to prove
cluster health and quantify replication lag, not to test version-skew command
compatibility.`,
	RunE: runWorkload,
}

func init() {
	cobra.OnInitialize(func() {
		viper.SetEnvPrefix("RUSS_CLIENT")
		viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
		viper.AutomaticEnv()
	})

	rootCmd.PersistentFlags().StringSlice("sentinel", []string{"localhost:26379"},
		"Sentinel addresses (comma-separated host:port)")
	rootCmd.PersistentFlags().String("cluster", "",
		"Sentinel master name to target (empty selects the only discovered master)")
	rootCmd.PersistentFlags().String("log-level", "info",
		"Log level: debug, info, warn, error")

	workloadCmd.Flags().Duration("interval", 200*time.Millisecond,
		"Cadence at which the writer, master reader, and replica observer each tick")
	workloadCmd.Flags().Duration("refresh-interval", 10*time.Second,
		"How often the observer re-asks sentinel for the current replica list")
	workloadCmd.Flags().Int("metrics-port", 9300,
		"HTTP port to expose Prometheus /metrics on (inside the russ network)")

	_ = viper.BindPFlags(rootCmd.PersistentFlags())
	_ = viper.BindPFlags(workloadCmd.Flags())

	rootCmd.AddCommand(listCmd, workloadCmd)
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("russ-client error")
	}
}

func configureLogging() {
	level := viper.GetString("log-level")
	if lvl, err := zerolog.ParseLevel(level); err == nil {
		zerolog.SetGlobalLevel(lvl)
	}
}

func sentinelAddrs() []string { return viper.GetStringSlice("sentinel") }
func clusterName() string     { return viper.GetString("cluster") }

func runList(cmd *cobra.Command, _ []string) error {
	configureLogging()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	masters, err := client.DiscoverMasters(ctx, sentinelAddrs())
	if err != nil {
		return err
	}
	if len(masters) == 0 {
		fmt.Println("No masters discovered.")
		return nil
	}
	fmt.Printf("%-20s  %s\n", "NAME", "ADDRESS")
	for _, m := range masters {
		fmt.Printf("%-20s  %s:%d\n", m.Name, m.Host, m.Port)
	}
	return nil
}

func runWorkload(cmd *cobra.Command, _ []string) error {
	configureLogging()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Resolve the target master once for logging; the runner's failover client
	// independently consults the sentinels and follows failovers on its own.
	discoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	masters, err := client.DiscoverMasters(discoCtx, sentinelAddrs())
	cancel()
	if err != nil {
		return err
	}
	chosen, err := client.PickMaster(masters, clusterName())
	if err != nil {
		return err
	}
	log.Info().
		Str("cluster", chosen.Name).
		Str("master", fmt.Sprintf("%s:%d", chosen.Host, chosen.Port)).
		Msg("selected master")

	reg := prometheus.NewRegistry()
	m := workload.NewMetrics()
	m.Register(reg)

	metricsAddr := fmt.Sprintf(":%d", viper.GetInt("metrics-port"))
	go func() {
		if err := workload.ServeMetrics(ctx, metricsAddr, reg); err != nil && err.Error() != "http: Server closed" {
			log.Warn().Err(err).Msg("metrics server stopped")
		}
	}()
	log.Info().Str("addr", metricsAddr).Msg("metrics endpoint up")

	runner := &workload.Runner{
		SentinelAddrs:   sentinelAddrs(),
		MasterName:      chosen.Name,
		Interval:        viper.GetDuration("interval"),
		RefreshInterval: viper.GetDuration("refresh-interval"),
		Metrics:         m,
	}
	return runner.Run(ctx)
}
