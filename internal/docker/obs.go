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

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/go-connections/nat"
)

//go:embed prometheus.yml
var prometheusConfig string

const (
	PrometheusImageTag         = "prom/prometheus:latest"
	RedisExporterImageTag      = "oliver006/redis_exporter:latest"
	PrometheusContainerName    = "russ-prometheus"
	RedisExporterContainerName = "russ-redis-exporter"

	// MetricsPort is the redis_exporter's metrics endpoint port.
	MetricsPort = 9121
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

// PullObservabilityImages pulls prom/prometheus and oliver006/redis_exporter
// if they're not already present locally.
func (m *Manager) PullObservabilityImages(ctx context.Context) error {
	for _, ref := range []string{PrometheusImageTag, RedisExporterImageTag} {
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
