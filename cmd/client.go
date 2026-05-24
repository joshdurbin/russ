package cmd

import (
	"context"
	"fmt"
	"strconv"

	"github.com/bigcommerce/russ/internal/docker"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Manage russ-client (writer/processor/api) containers",
	Long: `Build and run the three russ-client modes against a russ-managed Redis
Sentinel cluster:

  writer     wave-scaled pool of short-lived enqueuers emitting person:insert
             tasks onto the persons queue (asynq)
  processor  asynq server consuming the persons + stats queues; the persons
             handler writes durable Person/Address records to Redis and emits
             a stats:update task; the stats handler atomically updates the
             per-zip / per-state sorted sets
  api        HTTP REST endpoints (GET /api/persons, /api/persons/{ssn},
             /api/stats/by-zip, /api/stats/by-state) plus a Prometheus
             /metrics endpoint

All three containers join the russ Docker network, discover masters via
SENTINEL, and emit Prometheus metrics on port 9300. The api container
additionally publishes 9300 to host 127.0.0.1 so a browser/curl can reach
the JSON endpoints.`,
}

var clientLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List running russ-client containers (writer + processor + api)",
	RunE:  runClientLs,
}

func newClientBuildCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "build",
		Short: "Build the russ-client Docker image",
		RunE:  runClientBuild,
	}
	c.Flags().Bool("force-rebuild", false, "Force rebuild even if the image already exists")
	c.Flags().String("project-root", "", "Path to the russ source tree (default: auto-detect from cwd)")
	return c
}

var clientWorkloadCmd = &cobra.Command{
	Use:   "workload",
	Short: "Manage the full client stack (writer + processor + api)",
}

var clientWorkloadStartCmd = &cobra.Command{
	Use:   "start <cluster-name>",
	Short: "Start writer + processor + api containers for the given cluster",
	Args:  cobra.ExactArgs(1),
	RunE:  runWorkloadStart,
}

var clientWorkloadStopCmd = &cobra.Command{
	Use:   "stop [cluster-name]",
	Short: "Stop client containers; one cluster or all if no cluster given",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runWorkloadStop,
}

func init() {
	// Writer flags (passed through to the russ-client `run` subcommand,
	// which embeds writer goroutines coordinated via errgroup).
	clientWorkloadStartCmd.Flags().Int("min-clients", 2, "[writer] Enqueuer pool size at wave trough")
	clientWorkloadStartCmd.Flags().Int("max-clients", 50, "[writer] Enqueuer pool size at wave peak")
	clientWorkloadStartCmd.Flags().String("wave-period", "5m", "[writer] Full period of the load wave")
	clientWorkloadStartCmd.Flags().String("min-client-ttl", "2s", "[writer] Minimum enqueuer lifetime")
	clientWorkloadStartCmd.Flags().String("max-client-ttl", "30s", "[writer] Maximum enqueuer lifetime")
	clientWorkloadStartCmd.Flags().String("tick", "100ms", "[writer] Per-enqueuer interval between enqueue attempts")
	clientWorkloadStartCmd.Flags().Bool("burst", false, "[writer] Skip wave scaling; run at max-clients immediately")
	clientWorkloadStartCmd.Flags().Int("customer-pool", 5000, "[writer] Pre-generated customer pool size (smaller = higher repeat-customer rate)")
	clientWorkloadStartCmd.Flags().Int("catalog-size", 500, "[writer] Pre-generated product catalog size (smaller = stronger top-N concentration)")
	clientWorkloadStartCmd.Flags().Int("min-line-items", 1, "[writer] Minimum line items per order")
	clientWorkloadStartCmd.Flags().Int("max-line-items", 5, "[writer] Maximum line items per order")

	// Processor flags
	clientWorkloadStartCmd.Flags().Int("concurrency", 100, "[processor] Worker goroutines across both queues")

	// Subset toggles — useful for re-attaching to a running ingest, or
	// running just the api against an already-populated cluster.
	clientWorkloadStartCmd.Flags().Bool("no-writer", false, "Don't run the writer goroutine inside the container")
	clientWorkloadStartCmd.Flags().Bool("no-processor", false, "Don't run the processor goroutine inside the container")
	clientWorkloadStartCmd.Flags().Bool("no-api", false, "Don't run the api goroutine inside the container")

	// Shared
	clientWorkloadStartCmd.Flags().Int("metrics-port", 9300, "Container-internal port for /metrics + the JSON API")
	clientWorkloadStartCmd.Flags().Int("api-host-port", 9300, "Host loopback port to publish the API on (0 = no publish, container reachable only on the russ network)")
	clientWorkloadStartCmd.Flags().String("log-level", "info", "russ-client log level")

	clientWorkloadCmd.AddCommand(clientWorkloadStartCmd, clientWorkloadStopCmd)

	clientCmd.AddCommand(newClientBuildCmd())
	clientCmd.AddCommand(clientLsCmd)
	clientCmd.AddCommand(clientWorkloadCmd)
}

func runClientBuild(cmd *cobra.Command, _ []string) error {
	force, _ := cmd.Flags().GetBool("force-rebuild")
	projectRoot, _ := cmd.Flags().GetString("project-root")

	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	if err := dm.Ping(ctx); err != nil {
		return fmt.Errorf("Docker not reachable: %w", err)
	}
	return dm.BuildClientImage(ctx, force, projectRoot)
}

func runClientLs(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	containers, err := dm.ListClientContainers(ctx, "")
	if err != nil {
		return err
	}
	docker.PrintContainerTable(containers)
	return nil
}

func runWorkloadStart(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	if err := dm.Ping(ctx); err != nil {
		return fmt.Errorf("Docker not reachable: %w", err)
	}
	exists, err := dm.ImageExists(ctx, docker.ClientImageTag)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("image %s not found; run 'russ client build' first", docker.ClientImageTag)
	}

	instances, err := dm.ListClusterRedisInstances(ctx, clusterName)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		return fmt.Errorf("cluster %q has no Redis instances; create it with 'russ cluster create %s'", clusterName, clusterName)
	}

	sentinels, err := dm.ListSentinels(ctx)
	if err != nil {
		return err
	}
	if len(sentinels) == 0 {
		return fmt.Errorf("no sentinels running; start them with 'russ sentinel add'")
	}
	sentinelAddrs := make([]string, 0, len(sentinels))
	for _, s := range sentinels {
		sentinelAddrs = append(sentinelAddrs, fmt.Sprintf("%s:%d", s.Name, s.Port))
	}

	metricsPort, _ := cmd.Flags().GetInt("metrics-port")
	apiHostPort, _ := cmd.Flags().GetInt("api-host-port")
	logLevel, _ := cmd.Flags().GetString("log-level")

	extra := buildClientExtraArgs(cmd)

	name := docker.WorkloadContainerName(clusterName)
	if _, err := dm.GetContainer(ctx, name); err == nil {
		if err := dm.StopAndRemove(ctx, name); err != nil {
			return fmt.Errorf("remove pre-existing %s: %w", name, err)
		}
		log.Info().Str("name", name).Msg("removed pre-existing client container")
	}
	id, err := dm.StartWorkload(ctx, docker.StartWorkloadOpts{
		ContainerName: name,
		ClusterName:   clusterName,
		SentinelAddrs: sentinelAddrs,
		MetricsPort:   metricsPort,
		HostPort:      apiHostPort,
		LogLevel:      logLevel,
		ExtraArgs:     extra,
	})
	if err != nil {
		return err
	}
	log.Info().
		Str("name", name).
		Str("id", id[:12]).
		Str("api", fmt.Sprintf("http://127.0.0.1:%d/api/persons", apiHostPort)).
		Msg("client container started (writer + processor + api in one process)")
	return nil
}

// buildClientExtraArgs translates the host-side flags into the per-mode flags
// the russ-client `run` subcommand expects. Only forwarded if the user set
// them away from defaults (cobra's Changed() check); empty/zero values let
// the russ-client side pick its own defaults.
func buildClientExtraArgs(cmd *cobra.Command) []string {
	var args []string
	add := func(flag, val string) { args = append(args, "--"+flag, val) }
	flag := cmd.Flags()

	if flag.Changed("min-clients") {
		v, _ := flag.GetInt("min-clients")
		add("min-clients", strconv.Itoa(v))
	}
	if flag.Changed("max-clients") {
		v, _ := flag.GetInt("max-clients")
		add("max-clients", strconv.Itoa(v))
	}
	if flag.Changed("wave-period") {
		v, _ := flag.GetString("wave-period")
		add("wave-period", v)
	}
	if flag.Changed("min-client-ttl") {
		v, _ := flag.GetString("min-client-ttl")
		add("min-client-ttl", v)
	}
	if flag.Changed("max-client-ttl") {
		v, _ := flag.GetString("max-client-ttl")
		add("max-client-ttl", v)
	}
	if flag.Changed("tick") {
		v, _ := flag.GetString("tick")
		add("tick", v)
	}
	if b, _ := flag.GetBool("burst"); b {
		args = append(args, "--burst")
	}
	if flag.Changed("customer-pool") {
		v, _ := flag.GetInt("customer-pool")
		add("customer-pool", strconv.Itoa(v))
	}
	if flag.Changed("catalog-size") {
		v, _ := flag.GetInt("catalog-size")
		add("catalog-size", strconv.Itoa(v))
	}
	if flag.Changed("min-line-items") {
		v, _ := flag.GetInt("min-line-items")
		add("min-line-items", strconv.Itoa(v))
	}
	if flag.Changed("max-line-items") {
		v, _ := flag.GetInt("max-line-items")
		add("max-line-items", strconv.Itoa(v))
	}
	if flag.Changed("concurrency") {
		v, _ := flag.GetInt("concurrency")
		add("concurrency", strconv.Itoa(v))
	}
	if b, _ := flag.GetBool("no-writer"); b {
		args = append(args, "--no-writer")
	}
	if b, _ := flag.GetBool("no-processor"); b {
		args = append(args, "--no-processor")
	}
	if b, _ := flag.GetBool("no-api"); b {
		args = append(args, "--no-api")
	}
	return args
}

func runWorkloadStop(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	containers, err := dm.ListWorkloads(ctx)
	if err != nil {
		return err
	}
	if len(args) == 1 {
		cluster := args[0]
		filtered := make([]docker.ContainerInfo, 0, len(containers))
		for _, c := range containers {
			if c.ClusterName == cluster {
				filtered = append(filtered, c)
			}
		}
		containers = filtered
	}
	if len(containers) == 0 {
		log.Info().Msg("no matching client containers running")
		return nil
	}
	for _, c := range containers {
		if err := dm.StopAndRemove(ctx, c.Name); err != nil {
			log.Warn().Err(err).Str("name", c.Name).Msg("remove client container")
		} else {
			log.Info().Str("name", c.Name).Msg("removed client container")
		}
	}
	return nil
}
