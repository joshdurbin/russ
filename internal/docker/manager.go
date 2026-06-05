package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/docker/go-units"
	redisclient "github.com/bigcommerce/russ/internal/redis"
	"github.com/rs/zerolog/log"
)

// Per-container memory caps.
const (
	DefaultRedisContainerMemory      = 1024 * 1024 * 1024 // 1 GiB
	redisContainerHeadroomFactor     = 8
	DefaultClientContainerMemory     = 256 * 1024 * 1024 // 256 MiB
	DefaultPrometheusContainerMemory = 512 * 1024 * 1024 // 512 MiB
	DefaultExporterContainerMemory   = 32 * 1024 * 1024  //  32 MiB
	DefaultGrafanaContainerMemory    = 512 * 1024 * 1024 // 512 MiB
)

const (
	LabelManaged = "russ.managed"
	LabelRole    = "russ.role"
	LabelCluster = "russ.cluster"
	LabelVersion = "russ.version"
	LabelPort    = "russ.port"
	LabelName    = "russ.name"

	LabelTargetPort = "russ.target.port"
	LabelClientMode = "russ.client.mode"

	RoleMaster         = "master"
	RoleReplica        = "replica"
	RoleClientWorkload = "client-workload"
	RolePrometheus     = "prometheus"
	RoleRedisExporter  = "redis-exporter"
	RoleGrafana        = "grafana"
	// Legacy roles kept so `russ destroy` can sweep up old containers.
	RoleClientEnqueuer = "client-enqueuer"
	RoleClientDequeuer = "client-dequeuer"

	NetworkName = "russ"

	// RedisVersion is the Redis major version used for all new containers.
	RedisVersion = 8
)

func ImageForVersion(_ int) string {
	return "redis:8.0-alpine"
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

func (m *Manager) Ping(ctx context.Context) error {
	_, err := m.cli.Ping(ctx)
	return err
}

// PullImage ensures the Redis v8 image is available locally.
func (m *Manager) PullImage(ctx context.Context, version int) error {
	ref := ImageForVersion(version)
	exists, err := m.ImageExists(ctx, ref)
	if err != nil {
		return err
	}
	if exists {
		log.Debug().Str("image", ref).Msg("using cached image")
		return nil
	}
	log.Info().Str("image", ref).Msg("pulling image")
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
			log.Debug().Str("status", msg.Status).Str("progress", msg.Progress).Msg("image pull progress")
		}
	}
	log.Info().Str("image", ref).Msg("image pulled")
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
	log.Info().Str("network", NetworkName).Msg("created Docker network")
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

// StartRedisOpts configures a Redis server container.
type StartRedisOpts struct {
	ContainerName   string
	Version         int
	HostPort        int
	Role            string
	ClusterName     string
	MaxMemory       string
	MaxMemoryPolicy string
	Memory          int64
	Persistence     bool
	// ClusterMode enables Redis Cluster mode. The container announces itself
	// by ContainerName (resolved via Docker DNS on the russ network) so other
	// nodes can reach it, and by HostPort so clients on the host can too.
	ClusterMode bool
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

	var mounts []mount.Mount
	if opts.Persistence {
		volumeName := RedisDataVolumeName(opts.ClusterName, opts.HostPort)
		if err := m.EnsureVolume(ctx, volumeName, opts.ClusterName); err != nil {
			return "", err
		}
		labels[LabelDataVolume] = volumeName
		mounts = []mount.Mount{{
			Type:   mount.TypeVolume,
			Source: volumeName,
			Target: "/data",
		}}
	}

	cmd := []string{
		"redis-server",
		"--port", strconv.Itoa(opts.HostPort),
		"--bind", "0.0.0.0",
		"--loglevel", "notice",
		"--dir", "/data",
		"--client-output-buffer-limit", "replica", "192mb", "48mb", "60",
		"--repl-backlog-size", "32mb",
		"--repl-diskless-sync-delay", "0",
		"--repl-diskless-sync", "yes",
	}

	if opts.ClusterMode {
		cmd = append(cmd,
			"--cluster-enabled", "yes",
			"--cluster-config-file", "/data/nodes.conf",
			"--cluster-node-timeout", "5000",
			// Announce the container hostname so other nodes reach this node via
			// Docker DNS within the russ network.
			"--cluster-announce-hostname", opts.ContainerName,
			"--cluster-announce-port", strconv.Itoa(opts.HostPort),
			// Use hostname-based gossip (Redis 7.2+) so MOVED/ASK redirects carry
			// the container name, which Docker DNS resolves inside the network and
			// the host Dialer can map to 127.0.0.1:<port>.
			"--cluster-preferred-endpoint-type", "hostname",
		)
	}

	if opts.Persistence {
		cmd = append(cmd, "--appendonly", "yes", "--auto-aof-rewrite-percentage", "300")
	} else {
		cmd = append(cmd, "--appendonly", "no", "--save", "")
	}
	if opts.MaxMemory != "" {
		policy := opts.MaxMemoryPolicy
		if policy == "" {
			policy = "volatile-lru"
		}
		cmd = append(cmd, "--maxmemory", opts.MaxMemory, "--maxmemory-policy", policy)
	}

	memLimit := opts.Memory
	if memLimit == 0 {
		memLimit = DefaultRedisContainerMemory
	}
	if parsed, err := units.RAMInBytes(opts.MaxMemory); err == nil && parsed > 0 {
		needed := int64(redisContainerHeadroomFactor) * parsed
		if needed > memLimit {
			memLimit = needed
		}
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
			Mounts:      mounts,
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
	log.Debug().
		Str("name", opts.ContainerName).
		Str("id", resp.ID[:12]).
		Int("port", opts.HostPort).
		Str("role", opts.Role).
		Bool("cluster_mode", opts.ClusterMode).
		Msg("redis container started")
	return resp.ID, nil
}

// InitRedisCluster wires numMasters + (len(nodes)-numMasters) replica nodes into a
// functioning Redis Cluster. Call after all nodes are started and PING-ready.
//
// The first numMasters entries in nodes become masters; the rest become replicas
// distributed round-robin across those masters. Hash slots (0-16383) are divided
// evenly among masters.
func (m *Manager) InitRedisCluster(ctx context.Context, nodes []ContainerInfo, numMasters int) error {
	if numMasters < 3 {
		return fmt.Errorf("Redis Cluster requires at least 3 master nodes (requested %d)", numMasters)
	}
	if numMasters > len(nodes) {
		return fmt.Errorf("numMasters (%d) exceeds node count (%d)", numMasters, len(nodes))
	}

	// Fetch Docker network IPs — used for CLUSTER MEET so the nodes can
	// reach each other via the russ Docker network (not via 127.0.0.1).
	dockerIPs := make(map[string]string, len(nodes))
	for _, n := range nodes {
		ip, err := m.GetContainerNetworkIP(ctx, n.Name)
		if err != nil {
			return fmt.Errorf("get network IP for %s: %w", n.Name, err)
		}
		dockerIPs[n.Name] = ip
		log.Debug().Str("name", n.Name).Str("ip", ip).Msg("cluster node docker IP")
	}

	// Connect all nodes into one gossip ring: issue CLUSTER MEET from node[0]
	// to every other node using their Docker network IPs.
	for _, n := range nodes[1:] {
		ip := dockerIPs[n.Name]
		log.Debug().Str("target", n.Name).Str("ip", ip).Int("port", n.Port).Msg("CLUSTER MEET")
		if err := redisclient.ClusterMeet(ctx, nodes[0].Port, ip, n.Port); err != nil {
			return err
		}
	}

	// Wait until all nodes are visible in CLUSTER NODES on node[0].
	log.Info().Int("count", len(nodes)).Msg("waiting for cluster gossip to converge")
	if err := redisclient.WaitForClusterNodes(ctx, nodes[0].Port, len(nodes)); err != nil {
		return err
	}

	// Assign hash slots to masters (16384 slots split evenly, last master gets remainder).
	const totalSlots = 16384
	slotsPerMaster := totalSlots / numMasters
	for i := 0; i < numMasters; i++ {
		start := i * slotsPerMaster
		end := start + slotsPerMaster - 1
		if i == numMasters-1 {
			end = totalSlots - 1
		}
		log.Debug().Str("master", nodes[i].Name).Int("start", start).Int("end", end).Msg("CLUSTER ADDSLOTSRANGE")
		if err := redisclient.ClusterAddSlotsRange(ctx, nodes[i].Port, start, end); err != nil {
			return err
		}
	}

	// Collect master node IDs for CLUSTER REPLICATE.
	masterIDs := make([]string, numMasters)
	for i := 0; i < numMasters; i++ {
		id, err := redisclient.ClusterMyID(ctx, nodes[i].Port)
		if err != nil {
			return fmt.Errorf("get node ID for master %s: %w", nodes[i].Name, err)
		}
		masterIDs[i] = id
		log.Debug().Str("master", nodes[i].Name).Str("id", id[:8]).Msg("master node ID")
	}

	// Assign replicas round-robin across masters.
	// Wait for each replica to have received the target master's ID via gossip before
	// issuing REPLICATE — CLUSTER MEET convergence can lag a few hundred ms per node.
	for i := numMasters; i < len(nodes); i++ {
		masterIdx := (i - numMasters) % numMasters
		masterID := masterIDs[masterIdx]
		log.Debug().Str("replica", nodes[i].Name).Str("master", nodes[masterIdx].Name).Str("master_id", masterID[:8]).Msg("waiting for gossip then CLUSTER REPLICATE")
		if err := redisclient.WaitForClusterNodeID(ctx, nodes[i].Port, masterID); err != nil {
			return fmt.Errorf("replica %s waiting to see master: %w", nodes[i].Name, err)
		}
		if err := redisclient.ClusterReplicate(ctx, nodes[i].Port, masterID); err != nil {
			return err
		}
	}

	// Wait for the cluster to report state:ok.
	log.Info().Msg("waiting for cluster state:ok")
	if err := redisclient.WaitForClusterOK(ctx, nodes[0].Port); err != nil {
		return err
	}
	return nil
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

// StopAndRemove stops and force-removes a container plus its data volume (if any).
func (m *Manager) StopAndRemove(ctx context.Context, nameOrID string) error {
	var dataVolume string
	if inspect, err := m.cli.ContainerInspect(ctx, nameOrID); err == nil {
		if inspect.Config != nil {
			dataVolume = inspect.Config.Labels[LabelDataVolume]
		}
	}

	if err := m.cli.ContainerStop(ctx, nameOrID, container.StopOptions{}); err != nil {
		if !isNotRunning(err) {
			return fmt.Errorf("stop %s: %w", nameOrID, err)
		}
	}
	if err := m.cli.ContainerRemove(ctx, nameOrID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("remove %s: %w", nameOrID, err)
	}

	if dataVolume != "" {
		if err := m.RemoveVolume(ctx, dataVolume, false); err != nil {
			log.Warn().Err(err).Str("volume", dataVolume).Msg("remove data volume")
		}
	}
	return nil
}

// TailLogs returns the last n lines of stdout+stderr from the named container.
func (m *Manager) TailLogs(ctx context.Context, nameOrID string, n int) (string, error) {
	if n <= 0 {
		n = 50
	}
	rc, err := m.cli.ContainerLogs(ctx, nameOrID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Tail:       strconv.Itoa(n),
	})
	if err != nil {
		return "", fmt.Errorf("container logs %s: %w", nameOrID, err)
	}
	defer rc.Close()
	var out, errOut bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &errOut, rc); err != nil {
		return "", fmt.Errorf("demux container logs %s: %w", nameOrID, err)
	}
	combined := strings.TrimRight(out.String()+errOut.String(), "\n")
	return combined, nil
}

// WrapWaitError annotates a wait/readiness error with the failing container's recent log tail.
func (m *Manager) WrapWaitError(ctx context.Context, waitErr error, nameOrID string) error {
	if waitErr == nil {
		return nil
	}
	logs, logErr := m.TailLogs(ctx, nameOrID, 50)
	if logErr != nil || logs == "" {
		return waitErr
	}
	return fmt.Errorf("%w\n--- last 50 lines of %s ---\n%s", waitErr, nameOrID, logs)
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

// ListClusterContainers returns all containers belonging to the named cluster.
func (m *Manager) ListClusterContainers(ctx context.Context, clusterName string) ([]ContainerInfo, error) {
	return m.ListContainers(ctx, LabelCluster+"="+clusterName)
}

// ListClusterRedisInstances returns only the Redis master/replica containers for a cluster.
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

// InstanceContainerName returns the canonical container name for a Redis instance.
func InstanceContainerName(clusterName string, port int) string {
	return fmt.Sprintf("russ-%s-%d", clusterName, port)
}

// RemoveNetwork removes the russ Docker network.
func (m *Manager) RemoveNetwork(ctx context.Context) error {
	return m.cli.NetworkRemove(ctx, NetworkName)
}

// GetContainerNetworkIP returns the container's IP address on the russ Docker network.
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

// Client exposes the underlying docker client for one-off queries.
func (m *Manager) Client() *client.Client {
	return m.cli
}
