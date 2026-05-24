package docker

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/go-connections/nat"
)

//go:embed prometheus.yml
var prometheusConfig string

//go:embed grafana_datasource.yml
var grafanaDatasourceConfig string

//go:embed grafana_dashboards.yml
var grafanaDashboardsConfig string

//go:embed grafana_redis_dashboard_quickstart.json
var grafanaRedisDashboardQuickstart string

//go:embed grafana_russ_client_dashboard.json
var grafanaRussClientDashboard string

//go:embed grafana_russ_sentinel_dashboard.json
var grafanaRussSentinelDashboard string

const (
	PrometheusImageTag         = "prom/prometheus:latest"
	RedisExporterImageTag      = "oliver006/redis_exporter:latest"
	// Pinned to 12.x. Grafana 13's new unified-storage backend (bleve in-memory
	// indexes + resource versioning + dashboard-service reconciliation) is
	// substantially hungrier than earlier versions and was OOM-killing the
	// container at the previous 256 MiB cap, with logs showing repeated
	// "No last resource version found, starting from scratch" every ~60s
	// plus SQLITE_BUSY contention. 12.0.0 is the most recent stable line
	// without that churn.
	GrafanaImageTag            = "grafana/grafana-oss:12.0.0"
	PrometheusContainerName    = "russ-prometheus"
	RedisExporterContainerName = "russ-redis-exporter"
	GrafanaContainerName       = "russ-grafana"

	// MetricsPort is the redis_exporter's metrics endpoint port.
	MetricsPort = 9121

	// grafanaDatasourceUID is referenced by every panel in the embedded dashboard
	// after we substitute it in for the dashboard's import-time ${DS_PROM} placeholder.
	grafanaDatasourceUID = "russ-prometheus"
)

// PrometheusOpts configures the Prometheus container.
type PrometheusOpts struct {
	HostPort  int    // host port to publish the UI on; default 9090
	Retention string // tsdb retention.time, e.g. "2h"; default "2h"
	Memory    int64  // hard memory cap (bytes); 0 = DefaultPrometheusContainerMemory
}

// StartPrometheus writes the scrape config to ~/.russ/prometheus/prometheus.yml,
// bind-mounts it into a prom/prometheus container, also bind-mounts the Docker
// socket so Prometheus can run docker_sd_config, joins the russ network, and
// publishes the UI on 127.0.0.1:<HostPort>. Returns the container ID.
func (m *Manager) StartPrometheus(ctx context.Context, opts PrometheusOpts) (string, error) {
	if opts.HostPort == 0 {
		opts.HostPort = 9090
	}
	if opts.Retention == "" {
		opts.Retention = "2h"
	}
	memLimit := opts.Memory
	if memLimit == 0 {
		memLimit = DefaultPrometheusContainerMemory
	}

	confPath, err := writePrometheusConfig()
	if err != nil {
		return "", err
	}

	portSpec := nat.Port("9090/tcp")

	resp, err := m.cli.ContainerCreate(ctx,
		&container.Config{
			Image: PrometheusImageTag,
			Cmd: []string{
				"--config.file=/etc/prometheus/prometheus.yml",
				"--storage.tsdb.path=/prometheus",
				"--storage.tsdb.retention.time=" + opts.Retention,
				"--web.console.libraries=/usr/share/prometheus/console_libraries",
				"--web.console.templates=/usr/share/prometheus/consoles",
				"--web.enable-lifecycle",
			},
			Labels: map[string]string{
				LabelManaged: "true",
				LabelRole:    RolePrometheus,
				LabelName:    PrometheusContainerName,
			},
			ExposedPorts: nat.PortSet{portSpec: {}},
			// Run as root so the bind-mounted /var/run/docker.sock is readable
			// regardless of the daemon's socket group on the host VM.
			User: "0:0",
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				portSpec: []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: strconv.Itoa(opts.HostPort)}},
			},
			NetworkMode: container.NetworkMode(NetworkName),
			Mounts: []mount.Mount{
				{
					Type:     mount.TypeBind,
					Source:   confPath,
					Target:   "/etc/prometheus/prometheus.yml",
					ReadOnly: true,
				},
				{
					Type:     mount.TypeBind,
					Source:   "/var/run/docker.sock",
					Target:   "/var/run/docker.sock",
					ReadOnly: true,
				},
			},
			Resources: container.Resources{Memory: memLimit, MemorySwap: memLimit},
		},
		nil, nil,
		PrometheusContainerName,
	)
	if err != nil {
		return "", fmt.Errorf("create prometheus container: %w", err)
	}
	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("start prometheus container: %w", err)
	}
	return resp.ID, nil
}

// GrafanaOpts configures the Grafana container.
type GrafanaOpts struct {
	HostPort int   // host port to publish the UI on; default 3000
	Memory   int64 // hard memory cap (bytes); 0 = DefaultGrafanaContainerMemory
}

// StartGrafana materializes the provisioning tree under ~/.russ/grafana/,
// bind-mounts it into a grafana/grafana-oss container preconfigured with the
// Prometheus data source (uid=russ-prometheus pointing at the russ-prometheus
// service) and the oliver006 redis dashboard, and publishes the UI on
// 127.0.0.1:<HostPort>. Anonymous auth is enabled with Admin role so the
// dashboard is reachable without a login.
func (m *Manager) StartGrafana(ctx context.Context, opts GrafanaOpts) (string, error) {
	if opts.HostPort == 0 {
		opts.HostPort = 3000
	}
	memLimit := opts.Memory
	if memLimit == 0 {
		memLimit = DefaultGrafanaContainerMemory
	}

	provisioningDir, dashboardsDir, err := writeGrafanaConfig()
	if err != nil {
		return "", err
	}

	portSpec := nat.Port("3000/tcp")

	resp, err := m.cli.ContainerCreate(ctx,
		&container.Config{
			Image: GrafanaImageTag,
			Env: []string{
				// Skip the login screen: anyone hitting the UI lands as Admin.
				// Fine for local dev/sim; never appropriate for a real deployment.
				"GF_AUTH_ANONYMOUS_ENABLED=true",
				"GF_AUTH_ANONYMOUS_ORG_ROLE=Admin",
				"GF_AUTH_DISABLE_LOGIN_FORM=true",
				// Grafana's default analytics is noisy on first boot; silence it.
				"GF_ANALYTICS_REPORTING_ENABLED=false",
				"GF_ANALYTICS_CHECK_FOR_UPDATES=false",
			},
			Labels: map[string]string{
				LabelManaged: "true",
				LabelRole:    RoleGrafana,
				LabelName:    GrafanaContainerName,
			},
			ExposedPorts: nat.PortSet{portSpec: {}},
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				portSpec: []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: strconv.Itoa(opts.HostPort)}},
			},
			NetworkMode: container.NetworkMode(NetworkName),
			Mounts: []mount.Mount{
				{
					Type:     mount.TypeBind,
					Source:   provisioningDir,
					Target:   "/etc/grafana/provisioning",
					ReadOnly: true,
				},
				{
					Type:     mount.TypeBind,
					Source:   dashboardsDir,
					Target:   "/var/lib/grafana/dashboards",
					ReadOnly: true,
				},
			},
			Resources: container.Resources{Memory: memLimit, MemorySwap: memLimit},
		},
		nil, nil,
		GrafanaContainerName,
	)
	if err != nil {
		return "", fmt.Errorf("create grafana container: %w", err)
	}
	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("start grafana container: %w", err)
	}
	return resp.ID, nil
}

// StartSharedRedisExporter starts a single redis-exporter container operating
// in multi-target mode. Prometheus discovers Redis instances via docker_sd and
// routes each scrape through this exporter as /scrape?target=redis://...; the
// exporter dials targets on demand, so it stays stateless and doesn't need to
// be reconfigured when Redis instances are added or removed.
//
// Idempotent: returns the existing container ID if already running.
func (m *Manager) StartSharedRedisExporter(ctx context.Context) (string, error) {
	if existing, err := m.GetContainer(ctx, RedisExporterContainerName); err == nil {
		return existing.ID, nil
	}

	labels := map[string]string{
		LabelManaged: "true",
		LabelRole:    RoleRedisExporter,
		LabelName:    RedisExporterContainerName,
	}

	resp, err := m.cli.ContainerCreate(ctx,
		&container.Config{
			Image: RedisExporterImageTag,
			// No REDIS_ADDR — leave the exporter in multi-target mode so it
			// serves /scrape?target=<uri> for any Redis on the russ network.
			Labels: labels,
		},
		&container.HostConfig{
			NetworkMode:   container.NetworkMode(NetworkName),
			RestartPolicy: container.RestartPolicy{Name: "unless-stopped"},
			Resources:     container.Resources{Memory: DefaultExporterContainerMemory, MemorySwap: DefaultExporterContainerMemory},
		},
		nil, nil,
		RedisExporterContainerName,
	)
	if err != nil {
		return "", fmt.Errorf("create shared redis exporter: %w", err)
	}
	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("start shared redis exporter: %w", err)
	}
	return resp.ID, nil
}

// ListExporters returns every redis-exporter container. Normally this is just
// the single shared exporter; on a system mid-upgrade from the per-instance
// scheme it may also return legacy `russ-exporter-<cluster>-<port>` containers.
func (m *Manager) ListExporters(ctx context.Context) ([]ContainerInfo, error) {
	return m.ListContainers(ctx, LabelRole+"="+RoleRedisExporter)
}

// GetPrometheus returns the prometheus container, if running.
func (m *Manager) GetPrometheus(ctx context.Context) (ContainerInfo, bool, error) {
	list, err := m.ListContainers(ctx, LabelRole+"="+RolePrometheus)
	if err != nil {
		return ContainerInfo{}, false, err
	}
	if len(list) == 0 {
		return ContainerInfo{}, false, nil
	}
	return list[0], true, nil
}

// GetGrafana returns the grafana container, if running.
func (m *Manager) GetGrafana(ctx context.Context) (ContainerInfo, bool, error) {
	list, err := m.ListContainers(ctx, LabelRole+"="+RoleGrafana)
	if err != nil {
		return ContainerInfo{}, false, err
	}
	if len(list) == 0 {
		return ContainerInfo{}, false, nil
	}
	return list[0], true, nil
}

// PullObservabilityImages pulls prom/prometheus, oliver006/redis_exporter, and
// grafana/grafana-oss if they're not already present locally.
func (m *Manager) PullObservabilityImages(ctx context.Context) error {
	for _, ref := range []string{PrometheusImageTag, RedisExporterImageTag, GrafanaImageTag} {
		exists, err := m.ImageExists(ctx, ref)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		fmt.Printf("Pulling %s ...\n", ref)
		if err := m.pullImageRef(ctx, ref); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) pullImageRef(ctx context.Context, ref string) error {
	rc, err := m.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("image %s is not present locally and pull failed: %w\n  Hint: cache this image while online with `docker pull %s`", ref, err, ref)
	}
	defer rc.Close()
	dec := json.NewDecoder(rc)
	var msg struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	for dec.More() {
		if err := dec.Decode(&msg); err != nil {
			break
		}
		if msg.Error != "" {
			return fmt.Errorf("pull %s: %s", ref, msg.Error)
		}
	}
	_, _ = io.Copy(io.Discard, rc)
	return nil
}

// WritePrometheusConfig writes the embedded prometheus.yml to ~/.russ/prometheus/prometheus.yml
// and returns the path. Called by StartPrometheus on first start and by obs start to keep
// the config current when Prometheus is already running (before a hot reload).
func WritePrometheusConfig() error {
	_, err := writePrometheusConfig()
	return err
}

func writePrometheusConfig() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".russ", "prometheus")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create prometheus config dir: %w", err)
	}
	path := filepath.Join(dir, "prometheus.yml")
	if err := os.WriteFile(path, []byte(prometheusConfig), 0o644); err != nil {
		return "", fmt.Errorf("write prometheus config: %w", err)
	}
	return path, nil
}

// bundledDashboard describes one embedded Grafana dashboard JSON and the
// per-file fixups needed to wire it to russ-prometheus on materialization.
//
// Different dashboards encode their datasource reference differently:
//   - Many (oliver006 11692, helm-ha 11835, detailed 17507) carry a "${DS_*}"
//     placeholder that the Grafana import flow normally substitutes for a
//     UID. We do that substitution here so the panels resolve to russ-prometheus
//     without any post-import wiring.
//   - Some (quickstart 14091) carry a datasource template variable whose
//     default `current.value` needs to be set to russ-prometheus instead.
type bundledDashboard struct {
	filename     string   // file written into dashboardsDir
	content      string   // raw embedded JSON
	placeholders []string // ${DS_*} strings replaced with grafanaDatasourceUID
	// pinTemplateDatasource: if true, also rewrite the datasource template
	// variable's default value so the dropdown lands on our Prometheus.
	pinTemplateDatasource bool
}

var bundledDashboards = []bundledDashboard{
	{
		// https://grafana.com/grafana/dashboards/14091 (Redis Exporter Quickstart and Dashboard)
		filename:              "redis-quickstart.json",
		content:               grafanaRedisDashboardQuickstart,
		pinTemplateDatasource: true,
	},
	{
		// Hand-built dashboard for the russ-client workload metrics
		// (russ_client_writes_*, russ_client_deletes_*, russ_client_replica_reads_*,
		// russ_client_active_workers, russ_client_writer_target_bytes, etc.).
		// Built directly against datasource UID russ-prometheus so no placeholder
		// substitution is required.
		filename: "russ-client-workload.json",
		content:  grafanaRussClientDashboard,
	},
	{
		// Hand-built "Redis Sentinel" dashboard: sentinel counts by
		// version, per-master status/slave counts/quorum visibility, and per-sentinel
		// instance metrics (uptime, connected clients, memory). Uses the redis-sentinels
		// Prometheus job which scrapes sentinel containers through the shared exporter.
		filename: "russ-sentinel-fleet.json",
		content:  grafanaRussSentinelDashboard,
	},
}

// writeGrafanaConfig materializes the provisioning tree under ~/.russ/grafana/.
// Returns (provisioningDir, dashboardsDir) — both bind-mount sources for the
// Grafana container.
//
// Layout:
//
//	~/.russ/grafana/
//	├── provisioning/
//	│   ├── datasources/prometheus.yml
//	│   └── dashboards/dashboards.yml
//	└── dashboards/
//	    ├── redis-quickstart.json     (Grafana #14091 — Redis Exporter Quickstart)
//	    └── russ-client-workload.json (russ-client workload metrics, hand-built)
//
// The community dashboard has its datasource template variable's default
// pinned to russ-prometheus so panels resolve at load time without manual
// import wiring. The hand-built russ-client dashboard targets that UID directly.
func writeGrafanaConfig() (string, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", "", err
	}
	base := filepath.Join(home, ".russ", "grafana")
	provisioningDir := filepath.Join(base, "provisioning")
	dashboardsDir := filepath.Join(base, "dashboards")

	for _, sub := range []string{
		filepath.Join(provisioningDir, "datasources"),
		filepath.Join(provisioningDir, "dashboards"),
		dashboardsDir,
	} {
		if err := os.MkdirAll(sub, 0o755); err != nil {
			return "", "", fmt.Errorf("create grafana config dir %s: %w", sub, err)
		}
	}

	if err := os.WriteFile(
		filepath.Join(provisioningDir, "datasources", "prometheus.yml"),
		[]byte(grafanaDatasourceConfig), 0o644,
	); err != nil {
		return "", "", fmt.Errorf("write grafana datasource config: %w", err)
	}
	if err := os.WriteFile(
		filepath.Join(provisioningDir, "dashboards", "dashboards.yml"),
		[]byte(grafanaDashboardsConfig), 0o644,
	); err != nil {
		return "", "", fmt.Errorf("write grafana dashboards provider config: %w", err)
	}

	for _, b := range bundledDashboards {
		body := b.content
		for _, ph := range b.placeholders {
			body = strings.ReplaceAll(body, ph, grafanaDatasourceUID)
		}
		if b.pinTemplateDatasource {
			// Pin the datasource template variable's default to our Prometheus
			// so the dropdown lands on russ-prometheus on first load. Upstream
			// JSON has the template's current.text and current.value both
			// literally "prometheus" (Grafana's type filter, not a real UID),
			// which makes the variable resolve to *some* matching datasource
			// on load — leaving the dropdown visibly empty if any race occurs.
			// These two strings only appear in the template var, so the
			// substitution is safe (verified at materialization time).
			body = strings.ReplaceAll(body, `"text": "prometheus"`, `"text": "Prometheus"`)
			body = strings.ReplaceAll(body, `"value": "prometheus"`, `"value": "`+grafanaDatasourceUID+`"`)
		}
		if err := os.WriteFile(filepath.Join(dashboardsDir, b.filename), []byte(body), 0o644); err != nil {
			return "", "", fmt.Errorf("write grafana dashboard %s: %w", b.filename, err)
		}
	}

	return provisioningDir, dashboardsDir, nil
}

// ReloadPrometheus pokes Prometheus's /-/reload endpoint via the published
// host port. Only used if the config file is regenerated; docker_sd_config
// updates are auto-discovered, so this is rarely needed.
func ReloadPrometheus(hostPort int) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/-/reload", hostPort)
	resp, err := http.Post(url, "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("prometheus reload returned %s", resp.Status)
	}
	return nil
}
