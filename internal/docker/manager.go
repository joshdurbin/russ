package docker

import (
	"archive/tar"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	apitypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

//go:embed sentinel.Dockerfile
var sentinelDockerfileContent string

// Per-container memory caps. These bound runaway growth and stop Docker from
// reporting the host VM's full memory (often 2 GiB+) as each container's
// "limit". Override per-call via the Memory field on the Start*Opts struct.
const (
	DefaultRedisContainerMemory      = 128 * 1024 * 1024 // 128 MiB
	DefaultSentinelContainerMemory   = 64 * 1024 * 1024  //  64 MiB
	DefaultClientContainerMemory     = 256 * 1024 * 1024 // 256 MiB
	DefaultPrometheusContainerMemory = 512 * 1024 * 1024 // 512 MiB
	DefaultExporterContainerMemory   = 32 * 1024 * 1024  //  32 MiB
)

// DefaultRedisSourceVersion maps a major version (6 or 8) to the Redis source tag
// compiled into the patched sentinel image. Override with --redis-source-version.
var DefaultRedisSourceVersion = map[int]string{
	6: "6.2.17",
	8: "8.0.0",
}

// SentinelImageTag returns the local Docker image tag for the patched sentinel image.
func SentinelImageTag(version int) string {
	return fmt.Sprintf("russ-sentinel:%d", version)
}

const (
	LabelManaged = "russ.managed"
	LabelRole    = "russ.role"
	LabelCluster = "russ.cluster"
	LabelVersion = "russ.version"
	LabelPort    = "russ.port"
	LabelName    = "russ.name"

	// Used on the workload container so Prometheus's docker_sd_config can
	// build the scrape URL from network_ip + this port. (russ.target.port →
	// __meta_docker_container_label_russ_target_port in Prometheus.)
	LabelTargetPort = "russ.target.port"

	RoleSentinel        = "sentinel"
	RoleMaster          = "master"
	RoleReplica         = "replica"
	RoleClientWorkload  = "client-workload"
	RolePrometheus      = "prometheus"
	RoleRedisExporter   = "redis-exporter"
	// Legacy roles kept solely so `russ destroy` can still sweep up any
	// pre-workload-refactor enqueuer/dequeuer containers a user might have
	// left running. No new code creates these.
	RoleClientEnqueuer = "client-enqueuer"
	RoleClientDequeuer = "client-dequeuer"

	NetworkName = "russ"
)

func ImageForVersion(version int) string {
	switch version {
	case 6:
		return "redis:6.2-alpine"
	case 8:
		return "redis:8.0-alpine"
	default:
		panic(fmt.Sprintf("unsupported redis version: %d", version))
	}
}

type PortRange struct {
	Start, End int
}

func ParsePortRange(s string) (PortRange, error) {
	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return PortRange{}, fmt.Errorf("invalid port range %q, expected START-END", s)
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return PortRange{}, fmt.Errorf("invalid start port: %w", err)
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return PortRange{}, fmt.Errorf("invalid end port: %w", err)
	}
	if start >= end {
		return PortRange{}, fmt.Errorf("start port must be less than end port")
	}
	return PortRange{Start: start, End: end}, nil
}

// ContainerInfo holds metadata about a russ-managed container derived from Docker labels.
type ContainerInfo struct {
	ID          string
	Name        string
	Role        string
	Version     int
	Port        int
	ClusterName string
	Status      string
}

type Manager struct {
	cli *client.Client
}

func New() (*Manager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("connect to Docker: %w", err)
	}
	return &Manager{cli: cli}, nil
}

func (m *Manager) Close() error {
	return m.cli.Close()
}

// Ping verifies Docker is accessible.
func (m *Manager) Ping(ctx context.Context) error {
	_, err := m.cli.Ping(ctx)
	return err
}

// PullImage ensures the Redis image for the given major version is available
// locally. If the image is already cached (e.g., from a previous run), the
// pull is skipped entirely — making cluster create/instance add work offline
// against previously-fetched images. Only when no local copy exists do we
// reach out to the registry.
func (m *Manager) PullImage(ctx context.Context, version int) error {
	ref := ImageForVersion(version)
	exists, err := m.ImageExists(ctx, ref)
	if err != nil {
		return err
	}
	if exists {
		fmt.Printf("Using cached image %s.\n", ref)
		return nil
	}
	fmt.Printf("Pulling %s ...\n", ref)
	rc, err := m.cli.ImagePull(ctx, ref, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("image %s is not present locally and pull failed: %w\n  Hint: cache this image while online with `docker pull %s`", ref, err, ref)
	}
	defer rc.Close()
	dec := json.NewDecoder(rc)
	var msg struct {
		Status   string `json:"status"`
		Progress string `json:"progress"`
		Error    string `json:"error"`
	}
	for dec.More() {
		if err := dec.Decode(&msg); err != nil {
			io.Copy(io.Discard, rc)
			break
		}
		if msg.Error != "" {
			return fmt.Errorf("pull error: %s", msg.Error)
		}
		if msg.Progress != "" {
			fmt.Printf("\r  %s %s    ", msg.Status, msg.Progress)
		}
	}
	fmt.Printf("\r  done.                              \n")
	return nil
}

// EnsureNetwork creates the russ Docker bridge network if it does not exist.
func (m *Manager) EnsureNetwork(ctx context.Context) error {
	list, err := m.cli.NetworkList(ctx, network.ListOptions{
		Filters: filters.NewArgs(filters.Arg("name", NetworkName)),
	})
	if err != nil {
		return fmt.Errorf("list networks: %w", err)
	}
	for _, n := range list {
		if n.Name == NetworkName {
			return nil
		}
	}
	_, err = m.cli.NetworkCreate(ctx, NetworkName, network.CreateOptions{
		Driver: "bridge",
		Labels: map[string]string{LabelManaged: "true"},
	})
	if err != nil {
		return fmt.Errorf("create network %s: %w", NetworkName, err)
	}
	fmt.Printf("Created Docker network %q\n", NetworkName)
	return nil
}

// AllocatePort finds the first port in pr not already used by a russ container.
func (m *Manager) AllocatePort(ctx context.Context, pr PortRange) (int, error) {
	used, err := m.usedPorts(ctx)
	if err != nil {
		return 0, err
	}
	for p := pr.Start; p <= pr.End; p++ {
		if !used[p] {
			return p, nil
		}
	}
	return 0, fmt.Errorf("no free port in range %d-%d (all allocated)", pr.Start, pr.End)
}

func (m *Manager) usedPorts(ctx context.Context) (map[int]bool, error) {
	containers, err := m.ListContainers(ctx)
	if err != nil {
		return nil, err
	}
	used := make(map[int]bool, len(containers))
	for _, c := range containers {
		if c.Port > 0 {
			used[c.Port] = true
		}
	}
	return used, nil
}

type StartRedisOpts struct {
	ContainerName string
	Version       int
	HostPort      int
	Role          string
	ClusterName   string
	MaxMemory     string // Redis dataset cap, e.g. "10mb"; empty = no Redis-level limit
	Memory        int64  // Container hard memory limit (bytes); 0 = use DefaultRedisContainerMemory
}

// StartRedis starts a Redis server container on the russ network.
// Returns the container ID.
func (m *Manager) StartRedis(ctx context.Context, opts StartRedisOpts) (string, error) {
	img := ImageForVersion(opts.Version)
	portSpec := nat.Port(fmt.Sprintf("%d/tcp", opts.HostPort))

	labels := map[string]string{
		LabelManaged: "true",
		LabelRole:    opts.Role,
		LabelVersion: strconv.Itoa(opts.Version),
		LabelPort:    strconv.Itoa(opts.HostPort),
		LabelCluster: opts.ClusterName,
		LabelName:    opts.ContainerName,
	}

	cmd := []string{
		"redis-server",
		"--port", strconv.Itoa(opts.HostPort),
		"--bind", "0.0.0.0",
		"--loglevel", "notice",
	}
	if opts.MaxMemory != "" {
		cmd = append(cmd, "--maxmemory", opts.MaxMemory, "--maxmemory-policy", "allkeys-lru")
	}

	memLimit := opts.Memory
	if memLimit == 0 {
		memLimit = DefaultRedisContainerMemory
	}

	resp, err := m.cli.ContainerCreate(ctx,
		&container.Config{
			Image:  img,
			Cmd:    cmd,
			Labels: labels,
			ExposedPorts: nat.PortSet{
				portSpec: {},
			},
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				portSpec: []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: strconv.Itoa(opts.HostPort)}},
			},
			NetworkMode: container.NetworkMode(NetworkName),
			Resources:   container.Resources{Memory: memLimit, MemorySwap: memLimit},
		},
		nil, nil,
		opts.ContainerName,
	)
	if err != nil {
		return "", fmt.Errorf("create container %s: %w", opts.ContainerName, err)
	}
	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("start container %s: %w", opts.ContainerName, err)
	}
	return resp.ID, nil
}

// ImageExists reports whether a local Docker image with the given tag exists.
func (m *Manager) ImageExists(ctx context.Context, tag string) (bool, error) {
	list, err := m.cli.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", tag)),
	})
	if err != nil {
		return false, fmt.Errorf("image list: %w", err)
	}
	return len(list) > 0, nil
}

// BuildSentinelImage builds the tilt-patched sentinel image for the given version.
// redisSourceVersion is the exact Redis git tag to compile (e.g. "6.2.17", "8.6.3").
// If the image already exists and force is false, the build is skipped.
func (m *Manager) BuildSentinelImage(ctx context.Context, version int, redisSourceVersion string, force bool) error {
	tag := SentinelImageTag(version)

	if !force {
		exists, err := m.ImageExists(ctx, tag)
		if err != nil {
			return err
		}
		if exists {
			fmt.Printf("Sentinel image %s already exists (use --force-rebuild to rebuild).\n", tag)
			return nil
		}
	}

	fmt.Printf("Building sentinel image %s from Redis source %s...\n", tag, redisSourceVersion)
	fmt.Println("  (patching sentinel_tilt_trigger; this takes a few minutes on first build)")

	// Pack the embedded Dockerfile into an in-memory tar build context.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	content := []byte(sentinelDockerfileContent)
	if err := tw.WriteHeader(&tar.Header{
		Name:     "Dockerfile",
		Mode:     0644,
		Size:     int64(len(content)),
		Typeflag: tar.TypeReg,
	}); err != nil {
		return err
	}
	if _, err := tw.Write(content); err != nil {
		return err
	}
	tw.Close()

	baseImage := ImageForVersion(version)
	resp, err := m.cli.ImageBuild(ctx, &buf, apitypes.ImageBuildOptions{
		Tags:        []string{tag},
		BuildArgs:   map[string]*string{"REDIS_VERSION": &redisSourceVersion, "BASE_IMAGE": &baseImage},
		Remove:      true,
		ForceRemove: true,
		NoCache:     force,
	})
	if err != nil {
		return fmt.Errorf("image build %s: %w", tag, err)
	}
	defer resp.Body.Close()

	// Stream and print build output.
	dec := json.NewDecoder(resp.Body)
	var msg struct {
		Stream string `json:"stream"`
		Error  string `json:"error"`
	}
	for dec.More() {
		if err := dec.Decode(&msg); err != nil {
			break
		}
		if msg.Error != "" {
			return fmt.Errorf("build failed: %s", msg.Error)
		}
		if s := strings.TrimRight(msg.Stream, "\n"); s != "" {
			fmt.Println(" ", s)
		}
	}

	fmt.Printf("Built %s successfully.\n", tag)
	return nil
}

type StartSentinelOpts struct {
	ContainerName string
	Version       int
	HostPort      int
	Memory        int64 // Container hard memory limit (bytes); 0 = use DefaultSentinelContainerMemory
}

// StartSentinel starts a Redis Sentinel container on the russ network.
// Sentinels are started with minimal config; masters are added dynamically via SENTINEL MONITOR.
// The image used is the locally-built tilt-patched image (russ-sentinel:<version>).
func (m *Manager) StartSentinel(ctx context.Context, opts StartSentinelOpts) (string, error) {
	img := SentinelImageTag(opts.Version)
	portSpec := nat.Port(fmt.Sprintf("%d/tcp", opts.HostPort))

	labels := map[string]string{
		LabelManaged: "true",
		LabelRole:    RoleSentinel,
		LabelVersion: strconv.Itoa(opts.Version),
		LabelPort:    strconv.Itoa(opts.HostPort),
		LabelCluster: "",
		LabelName:    opts.ContainerName,
	}

	// Write a minimal sentinel config and start. Masters are added via SENTINEL MONITOR.
	// resolve-hostnames and announce-hostnames are required in Redis 6.x to accept
	// container names (not just IPs) in SENTINEL MONITOR, and so that sentinels
	// announce resolvable names to each other inside the Docker network.
	sentinelConf := fmt.Sprintf(
		"port %d\nbind 0.0.0.0\nlogfile \"\"\nsentinel resolve-hostnames yes\nsentinel announce-hostnames yes\n",
		opts.HostPort,
	)
	shellCmd := fmt.Sprintf(
		`printf '%%s' %s > /tmp/sentinel.conf && redis-sentinel /tmp/sentinel.conf`,
		shellQuote(sentinelConf),
	)

	memLimit := opts.Memory
	if memLimit == 0 {
		memLimit = DefaultSentinelContainerMemory
	}

	resp, err := m.cli.ContainerCreate(ctx,
		&container.Config{
			Image:  img,
			Cmd:    []string{"sh", "-c", shellCmd},
			Labels: labels,
			ExposedPorts: nat.PortSet{
				portSpec: {},
			},
		},
		&container.HostConfig{
			PortBindings: nat.PortMap{
				portSpec: []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: strconv.Itoa(opts.HostPort)}},
			},
			NetworkMode: container.NetworkMode(NetworkName),
			Resources:   container.Resources{Memory: memLimit, MemorySwap: memLimit},
		},
		nil, nil,
		opts.ContainerName,
	)
	if err != nil {
		return "", fmt.Errorf("create sentinel container %s: %w", opts.ContainerName, err)
	}
	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("start sentinel container %s: %w", opts.ContainerName, err)
	}
	return resp.ID, nil
}

// StopAndRemove stops and force-removes a container.
func (m *Manager) StopAndRemove(ctx context.Context, nameOrID string) error {
	if err := m.cli.ContainerStop(ctx, nameOrID, container.StopOptions{}); err != nil {
		// Ignore already-stopped errors; we'll force-remove below.
		if !isNotRunning(err) {
			return fmt.Errorf("stop %s: %w", nameOrID, err)
		}
	}
	if err := m.cli.ContainerRemove(ctx, nameOrID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("remove %s: %w", nameOrID, err)
	}
	return nil
}

// ListContainers returns all russ-managed containers, optionally narrowed by additional
// label filters in "key=value" form.
func (m *Manager) ListContainers(ctx context.Context, labelFilters ...string) ([]ContainerInfo, error) {
	args := filters.NewArgs(filters.Arg("label", LabelManaged+"=true"))
	for _, lf := range labelFilters {
		args.Add("label", lf)
	}
	list, err := m.cli.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: args,
	})
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}

	result := make([]ContainerInfo, 0, len(list))
	for _, c := range list {
		port, _ := strconv.Atoi(c.Labels[LabelPort])
		version, _ := strconv.Atoi(c.Labels[LabelVersion])
		name := strings.TrimPrefix(c.Names[0], "/")
		result = append(result, ContainerInfo{
			ID:          c.ID,
			Name:        name,
			Role:        c.Labels[LabelRole],
			Version:     version,
			Port:        port,
			ClusterName: c.Labels[LabelCluster],
			Status:      c.Status,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Port < result[j].Port
	})
	return result, nil
}

// GetContainer returns the single container with the given russ.name label, or an error.
func (m *Manager) GetContainer(ctx context.Context, name string) (ContainerInfo, error) {
	list, err := m.ListContainers(ctx, LabelName+"="+name)
	if err != nil {
		return ContainerInfo{}, err
	}
	if len(list) == 0 {
		return ContainerInfo{}, fmt.Errorf("no russ container named %q", name)
	}
	return list[0], nil
}

// ListSentinels returns all russ-managed sentinel containers.
func (m *Manager) ListSentinels(ctx context.Context) ([]ContainerInfo, error) {
	return m.ListContainers(ctx, LabelRole+"="+RoleSentinel)
}

// ListClusterContainers returns all containers belonging to the named cluster,
// including client containers (enqueuers/dequeuers) that target it.
func (m *Manager) ListClusterContainers(ctx context.Context, clusterName string) ([]ContainerInfo, error) {
	return m.ListContainers(ctx, LabelCluster+"="+clusterName)
}

// ListClusterRedisInstances returns only the Redis master/replica containers
// belonging to the named cluster — i.e., the data-plane nodes, not the load
// simulators. Use this whenever iterating "the instances in a cluster" for
// replication, failover, or topology operations.
func (m *Manager) ListClusterRedisInstances(ctx context.Context, clusterName string) ([]ContainerInfo, error) {
	all, err := m.ListClusterContainers(ctx, clusterName)
	if err != nil {
		return nil, err
	}
	out := make([]ContainerInfo, 0, len(all))
	for _, c := range all {
		if c.Role == RoleMaster || c.Role == RoleReplica {
			out = append(out, c)
		}
	}
	return out, nil
}

// InstanceContainerName returns the canonical container name for any Redis instance in a cluster.
// Names are port-based and role-agnostic: roles shift during failover but ports do not.
func InstanceContainerName(clusterName string, port int) string {
	return fmt.Sprintf("russ-%s-%d", clusterName, port)
}

// SentinelContainerName returns the canonical container name for a sentinel with a given port.
func SentinelContainerName(port int) string {
	return fmt.Sprintf("russ-sentinel-%d", port)
}

// RemoveNetwork removes the russ Docker network (call after all containers are removed).
func (m *Manager) RemoveNetwork(ctx context.Context) error {
	return m.cli.NetworkRemove(ctx, NetworkName)
}

// GetContainerNetworkIP returns the container's IP address on the russ Docker network.
// Using the network IP (rather than the container hostname) for SENTINEL MONITOR avoids
// relying on Docker's embedded DNS resolving at the exact moment the sentinel validates
// the command — Redis 6.x performs a synchronous resolve check on receipt.
func (m *Manager) GetContainerNetworkIP(ctx context.Context, containerName string) (string, error) {
	info, err := m.cli.ContainerInspect(ctx, containerName)
	if err != nil {
		return "", fmt.Errorf("inspect %s: %w", containerName, err)
	}
	net, ok := info.NetworkSettings.Networks[NetworkName]
	if !ok || net.IPAddress == "" {
		return "", fmt.Errorf("container %s has no IP on network %q", containerName, NetworkName)
	}
	return net.IPAddress, nil
}

// OS writes a shell-safe single-quoted string for use in sh -c commands.
func shellQuote(s string) string {
	// Replace each single quote with '\'' to safely embed in single-quoted string.
	escaped := strings.ReplaceAll(s, "'", `'\''`)
	return "'" + escaped + "'"
}

func isNotRunning(err error) bool {
	return err != nil && strings.Contains(err.Error(), "is not running")
}

// PrintContainerTable writes a formatted table of containers to stdout.
func PrintContainerTable(containers []ContainerInfo) {
	if len(containers) == 0 {
		fmt.Println("No containers found.")
		return
	}
	fmt.Printf("%-40s  %-16s  %-7s  %-5s  %-20s  %s\n",
		"NAME", "ROLE", "VERSION", "PORT", "CLUSTER", "STATUS")
	fmt.Println(strings.Repeat("-", 110))
	for _, c := range containers {
		cluster := c.ClusterName
		if cluster == "" {
			cluster = "-"
		}
		version := ""
		if c.Version > 0 {
			version = strconv.Itoa(c.Version)
		}
		port := ""
		if c.Port > 0 {
			port = strconv.Itoa(c.Port)
		}
		fmt.Printf("%-40s  %-16s  %-7s  %-5s  %-20s  %s\n",
			c.Name, c.Role, version, port, cluster, c.Status)
	}
}

// OS exposes the underlying docker client for one-off queries.
func (m *Manager) Client() *client.Client {
	return m.cli
}

// OS prints a simple status line to stderr.
func Logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "  → "+format+"\n", args...)
}
