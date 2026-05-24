package docker

import (
	"archive/tar"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	apitypes "github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/go-connections/nat"
)

// WorkloadContainerName returns the canonical container name for the
// russ-client container attached to the named cluster. One container per
// cluster runs all three modes (writer + processor + api) coordinated
// internally via errgroup.
func WorkloadContainerName(clusterName string) string {
	return fmt.Sprintf("russ-client-%s", clusterName)
}

// StartWorkloadOpts configures the russ-client container that runs the
// full writer + processor + api stack in a single process.
type StartWorkloadOpts struct {
	ContainerName string
	ClusterName   string
	SentinelAddrs []string

	MetricsPort int    // /metrics + JSON API port inside the container; default 9300
	HostPort    int    // host-side publish (127.0.0.1:HostPort); 0 = internal only
	LogLevel    string
	Memory      int64  // hard memory cap (bytes); 0 = DefaultClientContainerMemory

	// Extra args appended verbatim to the `run` subcommand. The host CLI
	// (cmd/client.go) builds these from its writer/processor/* flags.
	ExtraArgs []string
}

// StartWorkload launches a single russ-client container running the `run`
// subcommand, which orchestrates writer + processor + api in one process.
// The container is labeled so Prometheus (via docker_sd_config) discovers
// and scrapes its /metrics endpoint on MetricsPort within the russ network.
//
// Set HostPort to publish the metrics/JSON port to the host so the JSON API
// is reachable from the operator's browser/curl.
func (m *Manager) StartWorkload(ctx context.Context, opts StartWorkloadOpts) (string, error) {
	if opts.ClusterName == "" {
		return "", fmt.Errorf("cluster name is required")
	}
	if len(opts.SentinelAddrs) == 0 {
		return "", fmt.Errorf("no sentinel addresses provided")
	}
	if opts.MetricsPort == 0 {
		opts.MetricsPort = 9300
	}
	memLimit := opts.Memory
	if memLimit == 0 {
		memLimit = DefaultClientContainerMemory
	}

	args := []string{
		"run",
		"--sentinel", strings.Join(opts.SentinelAddrs, ","),
		"--cluster", opts.ClusterName,
		"--metrics-port", strconv.Itoa(opts.MetricsPort),
	}
	if opts.LogLevel != "" {
		args = append(args, "--log-level", opts.LogLevel)
	}
	args = append(args, opts.ExtraArgs...)

	labels := map[string]string{
		LabelManaged:    "true",
		LabelRole:       RoleClientWorkload,
		LabelCluster:    opts.ClusterName,
		LabelName:       opts.ContainerName,
		LabelTargetPort: strconv.Itoa(opts.MetricsPort),
	}

	cfg := &container.Config{
		Image:  ClientImageTag,
		Cmd:    args,
		Labels: labels,
	}
	hostCfg := &container.HostConfig{
		NetworkMode:   container.NetworkMode(NetworkName),
		RestartPolicy: container.RestartPolicy{Name: "unless-stopped"},
		Resources:     container.Resources{Memory: memLimit, MemorySwap: memLimit},
	}

	if opts.HostPort > 0 {
		portSpec := nat.Port(fmt.Sprintf("%d/tcp", opts.MetricsPort))
		cfg.ExposedPorts = nat.PortSet{portSpec: {}}
		hostCfg.PortBindings = nat.PortMap{
			portSpec: []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: strconv.Itoa(opts.HostPort)}},
		}
	}

	resp, err := m.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, opts.ContainerName)
	if err != nil {
		return "", fmt.Errorf("create client container %s: %w", opts.ContainerName, err)
	}
	if err := m.cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("start client container %s: %w", opts.ContainerName, err)
	}
	return resp.ID, nil
}

// ListWorkloads returns every russ-client container.
func (m *Manager) ListWorkloads(ctx context.Context) ([]ContainerInfo, error) {
	return m.ListContainers(ctx, LabelRole+"="+RoleClientWorkload)
}

// WorkloadAPIAddr returns the host-loopback URL the API for the given
// cluster is reachable at. Inspects the container's port bindings rather
// than reading a label, because the host-side publish port can be
// overridden at start time via --api-host-port and is not recorded in any
// label. Returns an empty string + nil error if the container exists but
// has no host-published port (i.e. --api-host-port=0).
func (m *Manager) WorkloadAPIAddr(ctx context.Context, clusterName string) (string, error) {
	name := WorkloadContainerName(clusterName)
	inspect, err := m.cli.ContainerInspect(ctx, name)
	if err != nil {
		return "", fmt.Errorf("inspect workload container %s: %w", name, err)
	}
	if inspect.NetworkSettings == nil {
		return "", nil
	}
	containerPort, _ := strconv.Atoi(inspect.Config.Labels[LabelTargetPort])
	if containerPort == 0 {
		containerPort = 9300
	}
	portSpec := nat.Port(fmt.Sprintf("%d/tcp", containerPort))
	bindings, ok := inspect.NetworkSettings.Ports[portSpec]
	if !ok || len(bindings) == 0 {
		return "", nil
	}
	for _, b := range bindings {
		if b.HostPort != "" {
			host := b.HostIP
			if host == "" || host == "0.0.0.0" {
				host = "127.0.0.1"
			}
			return fmt.Sprintf("http://%s:%s", host, b.HostPort), nil
		}
	}
	return "", nil
}

// ListClientContainers is a thin wrapper kept for callers that already use
// this name. The optional filter is ignored now that we run one container
// per cluster (no more sub-modes to filter on).
func (m *Manager) ListClientContainers(ctx context.Context, _ string) ([]ContainerInfo, error) {
	return m.ListContainers(ctx, LabelRole+"="+RoleClientWorkload)
}

// --- client image build ---

//go:embed client.Dockerfile
var clientDockerfileContent string

// ClientImageTag is the local Docker tag for the russ-client image.
const ClientImageTag = "russ-client:latest"

// BuildClientImage builds the russ-client image from the project tree at
// projectRoot. If projectRoot is empty, it's auto-detected by walking up from
// the current working directory to find the russ module's go.mod.
func (m *Manager) BuildClientImage(ctx context.Context, force bool, projectRoot string) error {
	if !force {
		exists, err := m.ImageExists(ctx, ClientImageTag)
		if err != nil {
			return err
		}
		if exists {
			fmt.Printf("Client image %s already exists (use --force-rebuild to rebuild).\n", ClientImageTag)
			return nil
		}
	}

	if projectRoot == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		root, err := findRussProjectRoot(cwd)
		if err != nil {
			return err
		}
		projectRoot = root
	}
	fmt.Printf("Building %s from %s...\n", ClientImageTag, projectRoot)

	buildCtx, err := buildClientContext(projectRoot)
	if err != nil {
		return fmt.Errorf("construct build context: %w", err)
	}

	resp, err := m.cli.ImageBuild(ctx, buildCtx, apitypes.ImageBuildOptions{
		Tags:        []string{ClientImageTag},
		Remove:      true,
		ForceRemove: true,
		// --force-rebuild means "rebuild even if the tag exists", not "bypass
		// the layer cache". Reusing the layer cache lets the go.mod / go.sum
		// COPY + `go mod download` step short-circuit when those files haven't
		// changed — important for offline use where the module proxy is
		// unreachable. To truly bypass the cache, remove the image first with
		// `docker image rm russ-client:latest` then build.
	})
	if err != nil {
		return fmt.Errorf("image build %s: %w", ClientImageTag, err)
	}
	defer resp.Body.Close()

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

	fmt.Printf("Built %s successfully.\n", ClientImageTag)
	return nil
}

// RemoveClientImage removes the russ-client image (best-effort).
func (m *Manager) RemoveClientImage(ctx context.Context) error {
	_, err := m.cli.ImageRemove(ctx, ClientImageTag, image.RemoveOptions{Force: true, PruneChildren: true})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "no such image") {
		return err
	}
	return nil
}

// findRussProjectRoot walks up from start looking for a go.mod whose first line
// is `module github.com/bigcommerce/russ`. The module-path check prevents
// latching onto an unrelated go.mod in a parent directory.
func findRussProjectRoot(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", err
	}
	for {
		mod := filepath.Join(dir, "go.mod")
		if data, err := os.ReadFile(mod); err == nil {
			if isRussModule(data) {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find russ project root; no go.mod with `module github.com/bigcommerce/russ` in any parent of %s (pass --project-root to override)", start)
		}
		dir = parent
	}
}

func isRussModule(modContent []byte) bool {
	scanner := bytes.SplitN(modContent, []byte("\n"), 2)
	if len(scanner) == 0 {
		return false
	}
	return strings.TrimSpace(string(scanner[0])) == "module github.com/bigcommerce/russ"
}

// buildClientContext tars the files needed to build russ-client: the embedded
// Dockerfile plus go.mod, go.sum, cmd/russ-client/, internal/client/.
func buildClientContext(projectRoot string) (io.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	if err := writeTarBytes(tw, "Dockerfile", []byte(clientDockerfileContent)); err != nil {
		return nil, err
	}

	for _, name := range []string{"go.mod", "go.sum"} {
		data, err := os.ReadFile(filepath.Join(projectRoot, name))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		if err := writeTarBytes(tw, name, data); err != nil {
			return nil, err
		}
	}

	for _, sub := range []string{
		filepath.Join("cmd", "russ-client"),
		filepath.Join("internal", "client"),
	} {
		full := filepath.Join(projectRoot, sub)
		err := filepath.WalkDir(full, func(p string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(projectRoot, p)
			if err != nil {
				return err
			}
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			return writeTarBytes(tw, filepath.ToSlash(rel), data)
		})
		if err != nil {
			return nil, fmt.Errorf("walk %s: %w", sub, err)
		}
	}

	if err := tw.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

func writeTarBytes(tw *tar.Writer, name string, data []byte) error {
	hdr := &tar.Header{
		Name:     name,
		Mode:     0o644,
		Size:     int64(len(data)),
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}
