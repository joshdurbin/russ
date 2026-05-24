package cmd

import (
	"context"
	"fmt"

	"github.com/bigcommerce/russ/internal/docker"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var obsCmd = &cobra.Command{
	Use:   "observability",
	Short: "Observability: Prometheus + shared redis-exporter + Grafana",
	Long: `Starts a Prometheus container, a single shared redis-exporter in
multi-target mode, and a Grafana container pre-provisioned with the Prometheus
data source and the oliver006 redis dashboard.

Prometheus discovers Redis instances via docker_sd_config filtered on
russ.role=master|replica and routes each scrape through the shared exporter as
/scrape?target=redis://<name>:<port>. Adding or removing Redis instances is
picked up automatically within ~5 seconds — no exporter sidecar to start, no
Prometheus reload needed.

Grafana is anonymous-admin (no login prompt) and the dashboard is loaded at
startup from a bind-mounted provisioning tree under ~/.russ/grafana/.`,
}

var obsStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start Prometheus, the shared redis-exporter, and Grafana",
	RunE:  runObsStart,
}

var obsStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop Prometheus, the shared redis-exporter (incl. any legacy per-instance exporters), and Grafana",
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
	obsStartCmd.Flags().Int("grafana-port", 3000, "Host port to publish the Grafana UI on")
	obsStartCmd.Flags().Int64("grafana-memory", 0, "Grafana container memory limit in bytes (0 = 512 MiB default)")

	obsCmd.AddCommand(obsStartCmd, obsStopCmd, obsLsCmd)
}

func runObsStart(cmd *cobra.Command, _ []string) error {
	retention, _ := cmd.Flags().GetString("retention")
	port, _ := cmd.Flags().GetInt("port")
	memory, _ := cmd.Flags().GetInt64("memory")
	grafanaPort, _ := cmd.Flags().GetInt("grafana-port")
	grafanaMemory, _ := cmd.Flags().GetInt64("grafana-memory")

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
				log.Warn().Err(err).Str("name", e.Name).Msg("remove legacy exporter")
			} else {
				log.Info().Str("name", e.Name).Msg("removed legacy per-instance exporter")
			}
		}
	}

	expID, err := dm.StartSharedRedisExporter(ctx)
	if err != nil {
		return err
	}
	log.Info().Str("name", docker.RedisExporterContainerName).Str("id", expID[:12]).Msg("shared redis-exporter started")

	if _, running, _ := dm.GetPrometheus(ctx); running {
		// Always rewrite the config file so any new scrape jobs (e.g. redis-sentinels)
		// are present on disk, then hot-reload Prometheus without restarting the container.
		if err := docker.WritePrometheusConfig(); err != nil {
			return fmt.Errorf("update prometheus config: %w", err)
		}
		if err := docker.ReloadPrometheus(port); err != nil {
			log.Warn().Err(err).Msg("prometheus reload failed; restart with 'russ observability stop && russ observability start' to pick up config changes")
		} else {
			log.Info().Msg("Prometheus already running; config rewritten and reloaded")
		}
	} else {
		promID, err := dm.StartPrometheus(ctx, docker.PrometheusOpts{
			HostPort:  port,
			Retention: retention,
			Memory:    memory,
		})
		if err != nil {
			return err
		}
		log.Info().
			Str("name", docker.PrometheusContainerName).
			Str("id", promID[:12]).
			Str("ui", fmt.Sprintf("http://127.0.0.1:%d", port)).
			Str("retention", retention).
			Msg("Prometheus started")
	}

	// Always (re)start Grafana so the binary's embedded dashboards and provisioning
	// are the source of truth. Grafana has no hot-reload for file provisioning
	// (updateIntervalSeconds: 0), so a container restart is the only way to pick
	// up new or changed dashboard files.
	if _, running, _ := dm.GetGrafana(ctx); running {
		if err := dm.StopAndRemove(ctx, docker.GrafanaContainerName); err != nil {
			return fmt.Errorf("stop grafana for restart: %w", err)
		}
		log.Info().Msg("stopped existing Grafana for restart with current dashboards")
	}
	grafID, err := dm.StartGrafana(ctx, docker.GrafanaOpts{
		HostPort: grafanaPort,
		Memory:   grafanaMemory,
	})
	if err != nil {
		return err
	}
	log.Info().
		Str("name", docker.GrafanaContainerName).
		Str("id", grafID[:12]).
		Str("ui", fmt.Sprintf("http://127.0.0.1:%d", grafanaPort)).
		Msg("Grafana started (anonymous admin; Prometheus data source + redis dashboards preprovisioned)")

	log.Info().Msg("Prometheus auto-discovers Redis instances via docker_sd (russ.role=master|replica); new instances are picked up within ~5s")
	return nil
}

func runObsStop(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	if graf, running, _ := dm.GetGrafana(ctx); running {
		if err := dm.StopAndRemove(ctx, graf.Name); err != nil {
			log.Warn().Err(err).Str("name", graf.Name).Msg("remove container")
		} else {
			log.Info().Str("name", graf.Name).Msg("removed container")
		}
	}

	if prom, running, _ := dm.GetPrometheus(ctx); running {
		if err := dm.StopAndRemove(ctx, prom.Name); err != nil {
			log.Warn().Err(err).Str("name", prom.Name).Msg("remove container")
		} else {
			log.Info().Str("name", prom.Name).Msg("removed container")
		}
	}

	exporters, err := dm.ListExporters(ctx)
	if err != nil {
		return err
	}
	for _, e := range exporters {
		if err := dm.StopAndRemove(ctx, e.Name); err != nil {
			log.Warn().Err(err).Str("name", e.Name).Msg("remove container")
		} else {
			log.Info().Str("name", e.Name).Msg("removed container")
		}
	}
	if len(exporters) == 0 {
		log.Info().Msg("no exporter containers were running")
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

	graf, running, _ := dm.GetGrafana(ctx)
	if running {
		fmt.Printf("Grafana:          %s  %s\n", graf.Name, graf.Status)
	} else {
		fmt.Println("Grafana:          not running")
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
