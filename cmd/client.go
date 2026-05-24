package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/bigcommerce/russ/internal/docker"
	"github.com/spf13/cobra"
)

var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "Manage russ-client workload containers",
	Long: `Build and run russ-client workload containers that drive synthetic load
against a russ-managed Redis Sentinel cluster. The workload runs a mix of
scenarios in parallel, each one exercising data structures or commands whose
availability differs across Redis 6.x → 7.x → 8.x.

All workload containers join the russ Docker network, discover masters via
SENTINEL MASTERS, and expose Prometheus metrics on port 9300 inside the
network. Prometheus (via 'russ observability start') auto-discovers and scrapes them.`,
}

var clientLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List running russ-client workload containers",
	RunE:  runClientLs,
}

func newClientBuildCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "build",
		Short: "Build the russ-client Docker image",
		Long: `Builds the russ-client image (tagged russ-client:latest) from the local
project tree using a multi-stage Dockerfile. The project root is auto-detected
by walking up from the current working directory; override with --project-root.`,
		RunE: runClientBuild,
	}
	c.Flags().Bool("force-rebuild", false, "Force rebuild even if the image already exists")
	c.Flags().String("project-root", "", "Path to the russ source tree (default: auto-detect from cwd)")
	return c
}

var clientWorkloadCmd = &cobra.Command{
	Use:   "workload",
	Short: "Manage workload containers",
}

var clientWorkloadStartCmd = &cobra.Command{
	Use:   "start <cluster-name>",
	Short: "Start a workload container targeting the given cluster",
	Args:  cobra.ExactArgs(1),
	RunE:  runWorkloadStart,
}

var clientWorkloadStopCmd = &cobra.Command{
	Use:   "stop [cluster-name]",
	Short: "Stop workload container(s); one cluster or all if no cluster given",
	Args:  cobra.MaximumNArgs(1),
	RunE:  runWorkloadStop,
}

func init() {
	clientWorkloadStartCmd.Flags().String("interval", "200ms",
		"Cadence at which the writer, master reader, and replica observer each tick")
	clientWorkloadStartCmd.Flags().String("refresh-interval", "10s",
		"How often the observer re-asks sentinel for the current replica list")
	clientWorkloadStartCmd.Flags().Int("metrics-port", 9300,
		"Metrics endpoint port inside the container (Prometheus scrapes via russ network)")
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
		return fmt.Errorf("no sentinels running; bootstrap them with 'russ sentinel bootstrap'")
	}
	sentinelAddrs := make([]string, 0, len(sentinels))
	for _, s := range sentinels {
		sentinelAddrs = append(sentinelAddrs, fmt.Sprintf("%s:%d", s.Name, s.Port))
	}

	interval, _ := cmd.Flags().GetString("interval")
	refreshInterval, _ := cmd.Flags().GetString("refresh-interval")
	metricsPort, _ := cmd.Flags().GetInt("metrics-port")
	logLevel, _ := cmd.Flags().GetString("log-level")

	containerName := docker.WorkloadContainerName(clusterName)
	if _, err := dm.GetContainer(ctx, containerName); err == nil {
		if err := dm.StopAndRemove(ctx, containerName); err != nil {
			return fmt.Errorf("remove pre-existing %s: %w", containerName, err)
		}
		fmt.Printf("  removed existing %s\n", containerName)
	}

	id, err := dm.StartWorkload(ctx, docker.StartWorkloadOpts{
		ContainerName:   containerName,
		ClusterName:     clusterName,
		SentinelAddrs:   sentinelAddrs,
		Interval:        interval,
		RefreshInterval: refreshInterval,
		MetricsPort:     metricsPort,
		LogLevel:        logLevel,
	})
	if err != nil {
		return err
	}
	fmt.Printf("  ✓ started %s  id=%.12s\n", containerName, id)
	fmt.Printf("    tail logs:  docker logs -f %s\n", containerName)
	fmt.Printf("    metrics:    http://127.0.0.1:9090/graph (search for russ_client_*)\n")
	return nil
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
		fmt.Println("No matching workload containers running.")
		return nil
	}
	for _, c := range containers {
		if err := dm.StopAndRemove(ctx, c.Name); err != nil {
			fmt.Fprintf(os.Stderr, "  warning: %v\n", err)
		} else {
			fmt.Printf("  ✓ removed %s\n", c.Name)
		}
	}
	return nil
}
