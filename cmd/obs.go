package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/bigcommerce/russ/internal/docker"
	"github.com/spf13/cobra"
)

var obsCmd = &cobra.Command{
	Use:   "observability",
	Short: "Observability: Prometheus + a shared redis-exporter",
	Long: `Starts a Prometheus container plus a single shared redis-exporter in
multi-target mode. Prometheus discovers Redis instances via docker_sd_config
filtered on russ.role=master|replica and routes each scrape through the shared
exporter as /scrape?target=redis://<name>:<port>. Adding or removing Redis
instances is picked up automatically within ~5 seconds — no exporter sidecar
to start, no Prometheus reload needed.`,
}

var obsStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start Prometheus and the shared redis-exporter",
	RunE:  runObsStart,
}

var obsStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop Prometheus and the shared redis-exporter (including any legacy per-instance exporters)",
	RunE:  runObsStop,
}

var obsLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List observability containers",
	RunE:  runObsLs,
}

func init() {
	obsStartCmd.Flags().String("retention", "2h", "Prometheus tsdb retention.time (e.g. 2h, 30m, 1d)")
	obsStartCmd.Flags().Int("port", 9090, "Host port to publish the Prometheus UI on")
	obsStartCmd.Flags().Int64("memory", 0, "Prometheus container memory limit in bytes (0 = 512 MiB default)")

	obsCmd.AddCommand(obsStartCmd, obsStopCmd, obsLsCmd)
}

func runObsStart(cmd *cobra.Command, _ []string) error {
	retention, _ := cmd.Flags().GetString("retention")
	port, _ := cmd.Flags().GetInt("port")
	memory, _ := cmd.Flags().GetInt64("memory")

	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	if err := dm.Ping(ctx); err != nil {
		return fmt.Errorf("Docker not reachable: %w", err)
	}
	if err := dm.EnsureNetwork(ctx); err != nil {
		return err
	}
	if err := dm.PullObservabilityImages(ctx); err != nil {
		return err
	}

	// Clean up any leftover per-instance exporters from the pre-shared design.
	exporters, _ := dm.ListExporters(ctx)
	for _, e := range exporters {
		if e.Name != docker.RedisExporterContainerName {
			if err := dm.StopAndRemove(ctx, e.Name); err != nil {
				fmt.Fprintf(os.Stderr, "  warning: remove legacy exporter %s: %v\n", e.Name, err)
			} else {
				fmt.Printf("  removed legacy per-instance exporter %s\n", e.Name)
			}
		}
	}

	expID, err := dm.StartSharedRedisExporter(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("  ✓ shared redis-exporter %s  id=%.12s\n", docker.RedisExporterContainerName, expID)

	if _, running, _ := dm.GetPrometheus(ctx); running {
		fmt.Println("  Prometheus already running.")
	} else {
		promID, err := dm.StartPrometheus(ctx, docker.PrometheusOpts{
			HostPort:  port,
			Retention: retention,
			Memory:    memory,
		})
		if err != nil {
			return err
		}
		fmt.Printf("  ✓ Prometheus %s  id=%.12s  (UI: http://127.0.0.1:%d)\n",
			docker.PrometheusContainerName, promID, port)
		fmt.Printf("    retention: %s\n", retention)
	}

	fmt.Println()
	fmt.Println("  Prometheus auto-discovers Redis instances via docker_sd (russ.role=master|replica).")
	fmt.Println("  New instances are picked up within ~5 seconds; no further action required.")
	return nil
}

func runObsStop(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	if prom, running, _ := dm.GetPrometheus(ctx); running {
		if err := dm.StopAndRemove(ctx, prom.Name); err != nil {
			fmt.Fprintf(os.Stderr, "  warning: %v\n", err)
		} else {
			fmt.Printf("  ✓ removed %s\n", prom.Name)
		}
	}

	exporters, err := dm.ListExporters(ctx)
	if err != nil {
		return err
	}
	for _, e := range exporters {
		if err := dm.StopAndRemove(ctx, e.Name); err != nil {
			fmt.Fprintf(os.Stderr, "  warning: %v\n", err)
		} else {
			fmt.Printf("  ✓ removed %s\n", e.Name)
		}
	}
	if len(exporters) == 0 {
		fmt.Println("  No exporter containers were running.")
	}
	return nil
}

func runObsLs(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	prom, running, _ := dm.GetPrometheus(ctx)
	if running {
		fmt.Printf("Prometheus:       %s  %s\n", prom.Name, prom.Status)
	} else {
		fmt.Println("Prometheus:       not running")
	}

	exporters, _ := dm.ListExporters(ctx)
	switch {
	case len(exporters) == 0:
		fmt.Println("redis-exporter:   not running")
	case len(exporters) == 1 && exporters[0].Name == docker.RedisExporterContainerName:
		fmt.Printf("redis-exporter:   %s  %s  (shared, multi-target)\n", exporters[0].Name, exporters[0].Status)
	default:
		fmt.Printf("redis-exporter:   %d container(s) (legacy per-instance — run 'russ observability start' to consolidate)\n", len(exporters))
		docker.PrintContainerTable(exporters)
	}
	return nil
}
