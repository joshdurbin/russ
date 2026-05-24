// russ-client is the workload simulator that runs inside Docker against a
// russ-managed Redis Sentinel fleet. It implements an e-commerce orders
// analytics engine over asynq: writers generate Order tasks, the processor
// consumes them and atomically updates only the analytics aggregates in
// Redis (no per-order durable storage), and a REST API surfaces those
// aggregates.
//
// Subcommands select the runtime mode:
//
//	russ-client list        — list sentinel-discovered masters and exit
//	russ-client writer      — wave-scaled order:process enqueuer
//	russ-client processor   — asynq server consuming orders_analytics
//	russ-client api         — HTTP REST + Prometheus /metrics
//	russ-client run         — all three in one process (default container mode)
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bigcommerce/russ/internal/client"
	"github.com/bigcommerce/russ/internal/client/orders"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

var rootCmd = &cobra.Command{
	Use:   "russ-client",
	Short: "E-commerce orders analytics workload simulator targeting a russ Sentinel-managed cluster",
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List masters discovered via sentinel and exit",
	RunE:  runList,
}

var writerCmd = &cobra.Command{
	Use:   "writer",
	Short: "Wave-scaled pool of short-lived enqueuers emitting order:process tasks",
	RunE:  runWriter,
}

var processorCmd = &cobra.Command{
	Use:   "processor",
	Short: "Asynq server consuming orders_analytics; applies aggregates atomically via Lua",
	RunE:  runProcessor,
}

var apiCmd = &cobra.Command{
	Use:   "api",
	Short: "HTTP REST API for the running analytics + Prometheus /metrics (standalone)",
	Long: `Endpoints:

  GET /api/analytics/summary            — totals + AOV + unique customers
  GET /api/analytics/top-products       — ?by=units|revenue&limit=N
  GET /api/analytics/top-categories     — ?by=units|revenue&limit=N
  GET /api/analytics/by-state           — ?by=count|revenue&limit=N
  GET /api/analytics/timeseries         — ?hours=N (default 24, max 72)
  GET /api/analytics/value-distribution — fixed-bucket histogram
  GET /metrics                          — Prometheus exposition`,
	RunE: runAPI,
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run writer + processor + api together in one process (default container mode)",
	Long: `Starts all three modes coordinated by a single errgroup. The
writer enqueues order:process tasks, the processor consumes them and atomically
updates analytics aggregates in Redis, and the api serves /api/analytics/* +
/metrics on --metrics-port.

If any of the three errors out, the errgroup cancels the others. Use
--no-writer / --no-processor / --no-api to disable any subset.`,
	RunE: runAll,
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
	rootCmd.PersistentFlags().Int("metrics-port", 9300,
		"HTTP port for /metrics + (for api/run modes) the JSON analytics endpoints")

	// writer
	writerCmd.Flags().Int("min-clients", 2, "Enqueuer pool size at wave trough")
	writerCmd.Flags().Int("max-clients", 50, "Enqueuer pool size at wave peak")
	writerCmd.Flags().Duration("wave-period", 5*time.Minute, "Full period of the load wave")
	writerCmd.Flags().Duration("min-client-ttl", 2*time.Second, "Minimum enqueuer lifetime")
	writerCmd.Flags().Duration("max-client-ttl", 30*time.Second, "Maximum enqueuer lifetime")
	writerCmd.Flags().Duration("tick", 100*time.Millisecond, "Per-enqueuer interval between enqueue attempts (0 = no throttle)")
	writerCmd.Flags().Bool("burst", false, "Skip wave scaling and run at max-clients immediately")
	writerCmd.Flags().Int("customer-pool", 5000, "Pre-generated customer pool size. Smaller = higher repeat-customer rate.")
	writerCmd.Flags().Int("catalog-size", 500, "Pre-generated product catalog size. Smaller = stronger top-N concentration.")
	writerCmd.Flags().Int("min-line-items", 1, "Minimum number of line items per order")
	writerCmd.Flags().Int("max-line-items", 5, "Maximum number of line items per order")
	writerCmd.Flags().Duration("task-retention", 15*time.Minute, "asynq.Retention for completed order:process tasks")
	writerCmd.Flags().Duration("task-timeout", 30*time.Second, "asynq.Timeout for each order:process task")
	writerCmd.Flags().Int("task-max-retry", 3, "asynq.MaxRetry for order:process tasks")

	// processor
	processorCmd.Flags().Int("concurrency", 100, "Worker goroutines on the orders_analytics queue")
	processorCmd.Flags().Duration("timeout", 30*time.Second, "Per-task timeout")
	processorCmd.Flags().Int("max-retry", 3, "asynq.MaxRetry for order:process tasks (processor side)")

	// `run` re-uses writer + processor flags, plus subset toggles.
	runCmd.Flags().AddFlagSet(writerCmd.Flags())
	runCmd.Flags().AddFlagSet(processorCmd.Flags())
	runCmd.Flags().Bool("no-writer", false, "Skip the writer goroutine")
	runCmd.Flags().Bool("no-processor", false, "Skip the processor goroutine")
	runCmd.Flags().Bool("no-api", false, "Skip the api goroutine")

	_ = viper.BindPFlags(rootCmd.PersistentFlags())
	_ = viper.BindPFlags(writerCmd.Flags())
	_ = viper.BindPFlags(processorCmd.Flags())
	_ = viper.BindPFlags(runCmd.Flags())

	rootCmd.AddCommand(listCmd, writerCmd, processorCmd, apiCmd, runCmd)
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
func metricsPort() int        { return viper.GetInt("metrics-port") }

func resolveMaster(ctx context.Context) (string, error) {
	discoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	masters, err := client.DiscoverMasters(discoCtx, sentinelAddrs())
	if err != nil {
		return "", err
	}
	chosen, err := client.PickMaster(masters, clusterName())
	if err != nil {
		return "", err
	}
	log.Info().
		Str("cluster", chosen.Name).
		Str("master", fmt.Sprintf("%s:%d", chosen.Host, chosen.Port)).
		Msg("selected master")
	return chosen.Name, nil
}

func runList(_ *cobra.Command, _ []string) error {
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

// serveMetricsOnly serves /metrics in a background goroutine. Used by the
// standalone writer + processor subcommands; the api + run subcommands
// serve /metrics inline with the JSON endpoints.
func serveMetricsOnly(ctx context.Context, reg *prometheus.Registry) {
	addr := fmt.Sprintf(":%d", metricsPort())
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	go func() {
		log.Info().Str("addr", addr).Msg("metrics endpoint up")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn().Err(err).Msg("metrics server stopped")
		}
	}()
}

func runWriter(_ *cobra.Command, _ []string) error {
	configureLogging()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	masterName, err := resolveMaster(ctx)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	m := orders.NewMetrics()
	m.Register(reg)
	serveMetricsOnly(ctx, reg)

	w := orders.NewWriter(writerConfigFromViper(masterName), m)
	defer w.Close()
	w.Run(ctx)
	return nil
}

func runProcessor(_ *cobra.Command, _ []string) error {
	configureLogging()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	masterName, err := resolveMaster(ctx)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	m := orders.NewMetrics()
	m.Register(reg)
	serveMetricsOnly(ctx, reg)

	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs(),
		PoolSize:      viper.GetInt("concurrency") + 16,
		DialTimeout:   3 * time.Second,
	})
	defer rdb.Close()

	store := orders.NewStore(rdb)
	p := orders.NewProcessor(processorConfigFromViper(masterName), store, m)
	return p.Run(ctx)
}

func runAPI(_ *cobra.Command, _ []string) error {
	configureLogging()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	masterName, err := resolveMaster(ctx)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	m := orders.NewMetrics()
	m.Register(reg)

	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs(),
		PoolSize:      32,
		DialTimeout:   3 * time.Second,
	})
	defer rdb.Close()

	store := orders.NewStore(rdb)
	api := orders.NewAPI(store, m, reg)
	return api.Serve(ctx, fmt.Sprintf(":%d", metricsPort()))
}

// runAll wires writer + processor + api into one process via errgroup.
// All three goroutines share the same Prometheus registry and the same
// Redis client pool (the writer opens its own via asynq.RedisFailoverClientOpt).
func runAll(_ *cobra.Command, _ []string) error {
	configureLogging()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	masterName, err := resolveMaster(ctx)
	if err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	m := orders.NewMetrics()
	m.Register(reg)

	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    masterName,
		SentinelAddrs: sentinelAddrs(),
		PoolSize:      viper.GetInt("concurrency") + 32,
		DialTimeout:   3 * time.Second,
	})
	defer rdb.Close()
	store := orders.NewStore(rdb)

	g, gctx := errgroup.WithContext(ctx)

	if !viper.GetBool("no-writer") {
		w := orders.NewWriter(writerConfigFromViper(masterName), m)
		g.Go(func() error {
			defer w.Close()
			w.Run(gctx)
			return nil
		})
	}
	if !viper.GetBool("no-processor") {
		p := orders.NewProcessor(processorConfigFromViper(masterName), store, m)
		g.Go(func() error { return p.Run(gctx) })
	}
	if !viper.GetBool("no-api") {
		api := orders.NewAPI(store, m, reg)
		g.Go(func() error {
			err := api.Serve(gctx, fmt.Sprintf(":%d", metricsPort()))
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		})
	}
	return g.Wait()
}

func writerConfigFromViper(masterName string) orders.WriterConfig {
	return orders.WriterConfig{
		SentinelAddrs: sentinelAddrs(),
		MasterName:    masterName,
		MinClients:    viper.GetInt("min-clients"),
		MaxClients:    viper.GetInt("max-clients"),
		WavePeriod:    viper.GetDuration("wave-period"),
		MinClientTTL:  viper.GetDuration("min-client-ttl"),
		MaxClientTTL:  viper.GetDuration("max-client-ttl"),
		Tick:          viper.GetDuration("tick"),
		Burst:         viper.GetBool("burst"),
		CustomerPool:  viper.GetInt("customer-pool"),
		CatalogSize:   viper.GetInt("catalog-size"),
		MinLineItems:  viper.GetInt("min-line-items"),
		MaxLineItems:  viper.GetInt("max-line-items"),
		TaskRetention: viper.GetDuration("task-retention"),
		TaskTimeout:   viper.GetDuration("task-timeout"),
		TaskMaxRetry:  viper.GetInt("task-max-retry"),
	}
}

func processorConfigFromViper(masterName string) orders.ProcessorConfig {
	return orders.ProcessorConfig{
		SentinelAddrs: sentinelAddrs(),
		MasterName:    masterName,
		Concurrency:   viper.GetInt("concurrency"),
		Timeout:       viper.GetDuration("timeout"),
		MaxRetry:      viper.GetInt("max-retry"),
	}
}
