package cmd

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/bigcommerce/russ/internal/docker"
	redisclient "github.com/bigcommerce/russ/internal/redis"
	"github.com/bigcommerce/russ/internal/state"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Manage Redis clusters",
}

var clusterCreateCmd = &cobra.Command{
	Use:   "create <cluster-name>",
	Short: "Create a Redis cluster and register it with all sentinels",
	Long: `Creates N Redis instances (1 master + N-1 replicas), configures replication,
and registers the cluster with every running sentinel via SENTINEL MONITOR.

The cluster name is used as the sentinel master name. Each instance is
assigned a port from the given range, with the first port becoming the master.`,
	Args: cobra.ExactArgs(1),
	RunE: runClusterCreate,
}

var clusterLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List all clusters",
	RunE:  runClusterLs,
}

var clusterRmCmd = &cobra.Command{
	Use:   "rm <cluster-name>",
	Short: "Remove a cluster and all its instances",
	Long: `Tells all sentinels to stop monitoring the cluster, then stops and removes
all Redis containers belonging to the cluster.`,
	Args: cobra.ExactArgs(1),
	RunE: runClusterRm,
}

var clusterStatusCmd = &cobra.Command{
	Use:   "status <cluster-name>",
	Short: "Show the upgrade lifecycle state and instance topology for a cluster",
	Args:  cobra.ExactArgs(1),
	RunE:  runClusterStatus,
}

var clusterIsolateV6ReplicasCmd = &cobra.Command{
	Use:   "isolate-v6-replicas <cluster-name>",
	Short: "Issue REPLICAOF NO ONE on v6 replicas and reset sentinels (v6→v8 upgrade step)",
	Long: `Issues REPLICAOF NO ONE on every v6 replica in the cluster, then issues
SENTINEL RESET on all sentinels so they rediscover the live topology.

Advances the upgrade lifecycle state to ReplicationBroken.`,
	Args: cobra.ExactArgs(1),
	RunE: runClusterIsolateV6Replicas,
}

var clusterLifecycleCmd = &cobra.Command{
	Use:   "lifecycle <cluster-name>",
	Short: "Walk the upgrade FSM and mark the cluster's current position",
	Long: `Renders every state in the v6→v8 upgrade FSM with markers for what's
already complete (✓), the current state (▶), and what's still pending (○).
Between each pair of states the FSM trigger and the russ command that fires it
are shown.`,
	Args: cobra.ExactArgs(1),
	RunE: runClusterLifecycle,
}

var clusterMonitorCmd = &cobra.Command{
	Use:   "monitor <cluster-name>",
	Short: "Stream MONITOR output from the sentinel-reported master (auto-follows failovers)",
	Long: `Opens a go-redis FailoverClient against the named cluster's sentinel fleet,
runs the MONITOR command on the current master, and streams every executed
command to stdout until Ctrl-C.

Because it's a FailoverClient, a sentinel-driven failover during monitoring
transparently switches the stream to the new master.

Note: MONITOR has nontrivial performance impact on the monitored server —
every executed command is duplicated to the monitor stream. Use sparingly
under heavy load.`,
	Args: cobra.ExactArgs(1),
	RunE: runClusterMonitor,
}

func init() {
	clusterCreateCmd.Flags().Int("version", 6, "Redis major version: 6 or 8 (default 6)")
	clusterCreateCmd.Flags().Int("count", 3, "Total instance count (1 master + N-1 replicas)")
	clusterCreateCmd.Flags().String("port-range", "6380-6500", "Port range for instance allocation")
	clusterCreateCmd.Flags().String("max-memory", "64mb", "maxmemory limit applied to all instances")

	clusterCmd.AddCommand(clusterCreateCmd)
	clusterCmd.AddCommand(clusterLsCmd)
	clusterCmd.AddCommand(clusterRmCmd)
	clusterCmd.AddCommand(clusterStatusCmd)
	clusterCmd.AddCommand(clusterIsolateV6ReplicasCmd)
	clusterCmd.AddCommand(clusterLifecycleCmd)
	clusterCmd.AddCommand(clusterMonitorCmd)
}

func runClusterCreate(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	version, _ := cmd.Flags().GetInt("version")
	count, _ := cmd.Flags().GetInt("count")
	portRangeStr, _ := cmd.Flags().GetString("port-range")
	maxMemory, _ := cmd.Flags().GetString("max-memory")

	if version != 6 && version != 8 {
		return fmt.Errorf("--version must be 6 or 8")
	}
	if count < 1 {
		return fmt.Errorf("--count must be at least 1")
	}

	pr, err := docker.ParsePortRange(portRangeStr)
	if err != nil {
		return err
	}

	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	if err := dm.Ping(ctx); err != nil {
		return fmt.Errorf("Docker not reachable: %w", err)
	}

	// Check if cluster already exists.
	existing, err := dm.ListClusterRedisInstances(ctx, clusterName)
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		return fmt.Errorf("cluster %q already exists (%d instances); use 'russ instance add' to grow it", clusterName, len(existing))
	}

	if err := dm.EnsureNetwork(ctx); err != nil {
		return err
	}
	if err := dm.PullImage(ctx, version); err != nil {
		return err
	}

	fmt.Printf("Creating cluster %q: %d instance(s) on Redis %d.x (maxmemory=%s)\n",
		clusterName, count, version, maxMemory)

	// Allocate all ports up front so we can plan the topology before starting anything.
	ports := make([]int, count)
	for i := 0; i < count; i++ {
		p, err := dm.AllocatePort(ctx, pr)
		if err != nil {
			return fmt.Errorf("allocate port for instance %d: %w", i+1, err)
		}
		ports[i] = p
		// Temporarily mark port as used by bumping pr.Start so the next allocation
		// doesn't re-use the same port (AllocatePort scans live Docker labels, but the
		// container hasn't been created yet).
		pr.Start = p + 1
	}

	masterPort := ports[0]
	masterName := docker.InstanceContainerName(clusterName, masterPort)

	// Start master.
	masterID, err := dm.StartRedis(ctx, docker.StartRedisOpts{
		ContainerName: masterName,
		Version:       version,
		HostPort:      masterPort,
		Role:          docker.RoleMaster,
		ClusterName:   clusterName,
		MaxMemory:     maxMemory,
	})
	if err != nil {
		return err
	}
	if err := redisclient.WaitForReady(ctx, masterPort); err != nil {
		return err
	}
	fmt.Printf("  ✓ master  %s  port=%d  id=%.12s\n", masterName, masterPort, masterID)

	// Start replicas and configure replication.
	for _, replicaPort := range ports[1:] {
		replicaName := docker.InstanceContainerName(clusterName, replicaPort)
		id, err := dm.StartRedis(ctx, docker.StartRedisOpts{
			ContainerName: replicaName,
			Version:       version,
			HostPort:      replicaPort,
			Role:          docker.RoleReplica,
			ClusterName:   clusterName,
			MaxMemory:     maxMemory,
		})
		if err != nil {
			return err
		}
		if err := redisclient.WaitForReady(ctx, replicaPort); err != nil {
			return err
		}
		if err := redisclient.ConfigureReplica(ctx, replicaPort, masterName, masterPort); err != nil {
			return err
		}
		if err := redisclient.WaitForReplication(ctx, replicaPort, masterPort); err != nil {
			return err
		}
		fmt.Printf("  ✓ replica %s  port=%d  id=%.12s\n", replicaName, replicaPort, id)
	}

	// Register with every running sentinel.
	// Use the master's Docker network IP rather than its container hostname: Redis 6.x
	// performs a synchronous DNS lookup when it receives SENTINEL MONITOR, and the
	// timing of Docker's embedded DNS can cause intermittent "Invalid IP address or
	// hostname" errors. The IP is stable for the lifetime of the container.
	masterIP, err := dm.GetContainerNetworkIP(ctx, masterName)
	if err != nil {
		return fmt.Errorf("get master network IP: %w", err)
	}

	sentinels, err := dm.ListSentinels(ctx)
	if err != nil {
		return err
	}
	if len(sentinels) == 0 {
		fmt.Println("\nWarning: no sentinels running. Run 'russ sentinel bootstrap' to add sentinels.")
	} else {
		quorum := len(sentinels)/2 + 1
		fmt.Printf("\nRegistering cluster %q with %d sentinel(s) (quorum=%d)...\n", clusterName, len(sentinels), quorum)
		fmt.Printf("  Master: %s → %s:%d\n", masterName, masterIP, masterPort)
		for _, s := range sentinels {
			if err := redisclient.SentinelMonitor(ctx, s.Port, clusterName, masterIP, masterPort, quorum); err != nil {
				return fmt.Errorf("SENTINEL MONITOR on %s: %w", s.Name, err)
			}
			fmt.Printf("  ✓ sentinel %s registered\n", s.Name)
		}
	}

	// Initialise upgrade lifecycle state.
	var initialState state.State
	if version == 6 {
		initialState = state.StateAllV6
	} else {
		initialState = state.StateDone // Started on v8, no upgrade path needed.
	}
	if err := state.Save(clusterName, initialState); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not persist cluster state: %v\n", err)
	}

	fmt.Printf("\nCluster %q ready. Upgrade state: %s\n", clusterName, initialState)
	printNextUpgradeStep(ctx, clusterName)
	return nil
}

func runClusterLs(cmd *cobra.Command, _ []string) error {
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	// Find all masters to identify distinct clusters.
	masters, err := dm.ListContainers(ctx, docker.LabelRole+"="+docker.RoleMaster)
	if err != nil {
		return err
	}
	if len(masters) == 0 {
		fmt.Println("No clusters found.")
		return nil
	}

	sentinels, _ := dm.ListSentinels(ctx)

	fmt.Printf("%-20s  %-7s  %-9s  %-12s  %-5s  %s\n",
		"CLUSTER", "VERSION", "INSTANCES", "MASTER PORT", "SENTS", "UPGRADE STATE")
	fmt.Println(strings.Repeat("-", 80))

	for _, m := range masters {
		instances, _ := dm.ListClusterRedisInstances(ctx, m.ClusterName)
		upgradeState, _ := state.Load(m.ClusterName)

		// Check sentinel awareness: query any one sentinel for master info.
		sentinelAware := "-"
		for _, s := range sentinels {
			info, err := redisclient.SentinelGetMaster(ctx, s.Port, m.ClusterName)
			if err == nil {
				sentinelAware = fmt.Sprintf("%d", info.Port)
				break
			}
		}

		fmt.Printf("%-20s  %-7d  %-9d  %-12s  %-5d  %s\n",
			m.ClusterName, m.Version, len(instances), sentinelAware, len(sentinels), upgradeState)
	}
	return nil
}

func runClusterRm(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	instances, err := dm.ListClusterContainers(ctx, clusterName)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		return fmt.Errorf("cluster %q not found", clusterName)
	}

	// Tell all sentinels to stop monitoring this cluster.
	sentinels, _ := dm.ListSentinels(ctx)
	for _, s := range sentinels {
		if err := redisclient.SentinelRemove(ctx, s.Port, clusterName); err != nil {
			fmt.Fprintf(os.Stderr, "  warning: SENTINEL REMOVE on %s: %v\n", s.Name, err)
		} else {
			fmt.Printf("  ✓ sentinel %s: removed monitor for %q\n", s.Name, clusterName)
		}
	}

	// Stop and remove all cluster containers.
	for _, ci := range instances {
		if err := dm.StopAndRemove(ctx, ci.Name); err != nil {
			fmt.Fprintf(os.Stderr, "  warning: remove %s: %v\n", ci.Name, err)
		} else {
			fmt.Printf("  ✓ removed %s\n", ci.Name)
		}
	}

	// Clean up persisted upgrade state.
	_ = state.Delete(clusterName)

	fmt.Printf("Cluster %q removed.\n", clusterName)
	return nil
}

func runClusterStatus(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	instances, err := dm.ListClusterRedisInstances(ctx, clusterName)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		return fmt.Errorf("cluster %q not found", clusterName)
	}

	upgradeState, _ := state.Load(clusterName)

	sentinels, _ := dm.ListSentinels(ctx)

	// Get current master from sentinel.
	var currentMasterPort int
	for _, s := range sentinels {
		info, err := redisclient.SentinelGetMaster(ctx, s.Port, clusterName)
		if err == nil {
			currentMasterPort = info.Port
			break
		}
	}

	machine, _ := state.NewMachine(clusterName)
	permitted, _ := machine.PermittedTriggers(ctx)

	fmt.Printf("Cluster:        %s\n", clusterName)
	fmt.Printf("Upgrade state:  %s\n", upgradeState)
	if len(permitted) > 0 {
		fmt.Printf("Next triggers:  %s\n", strings.Join(permitted, ", "))
	}
	fmt.Printf("Sentinels:      %d\n\n", len(sentinels))

	printInstanceTable(ctx, instances, currentMasterPort)
	return nil
}

// triggerCommands maps each FSM trigger to the human-facing russ command that
// fires it. This is documentation — the FSM topology itself is discovered at
// runtime via state.WalkLifecycle, but the trigger→command mapping isn't part
// of the FSM and lives here. A missing entry just renders as "(no command)".
var triggerCommands = map[string]string{
	state.TriggerAddV8Instance:     "russ instance add <cluster> --version=8",
	state.TriggerFailover:          "russ sentinel failover <cluster>",
	state.TriggerBreakReplication:  "russ cluster isolate-v6-replicas <cluster>",
	state.TriggerDestroyV6:         "russ instance rm <last v6 instance>   (auto-advances)",
	state.TriggerAddV8Sentinels:    "russ sentinel bootstrap --version=8",
	state.TriggerRemoveV6Sentinels: "russ sentinel rm <last v6 sentinel>   (auto-advances)",
}

// printNextUpgradeStep emits a single-line hint for the next FSM trigger and
// the russ command that fires it, derived from the cluster's current state.
// Silent if the state is terminal or no command is mapped.
func printNextUpgradeStep(ctx context.Context, clusterName string) {
	machine, err := state.NewMachine(clusterName)
	if err != nil {
		return
	}
	triggers, err := machine.PermittedTriggers(ctx)
	if err != nil {
		return
	}
	for _, t := range triggers {
		cmd, ok := triggerCommands[t]
		if !ok {
			continue
		}
		fmt.Printf("\nNext upgrade step: %s\n", strings.ReplaceAll(cmd, "<cluster>", clusterName))
		return
	}
}

func runClusterLifecycle(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	ctx := context.Background()

	current, err := state.Load(clusterName)
	if err != nil {
		return err
	}

	order, transitions, err := state.WalkLifecycle(ctx)
	if err != nil {
		return fmt.Errorf("walk lifecycle: %w", err)
	}

	currentIdx := -1
	for i, s := range order {
		if s == current {
			currentIdx = i
			break
		}
	}
	if currentIdx < 0 {
		return fmt.Errorf("current state %q is not reachable from the FSM's initial state — internal/state/machine.go may be inconsistent", current)
	}

	fmt.Printf("Cluster:  %s\n", clusterName)
	fmt.Printf("State:    %s\n\n", current)

	for i, s := range order {
		var mark, suffix string
		switch {
		case i < currentIdx:
			mark = "✓"
		case i == currentIdx:
			mark = "▶"
			suffix = "   (current)"
		default:
			mark = "○"
		}
		fmt.Printf("  %s  %s%s\n", mark, s, suffix)

		for _, tr := range transitions[s] {
			command := triggerCommands[tr.Trigger]
			if command == "" {
				command = "(no command mapping)"
			}
			nextHint := ""
			if i == currentIdx {
				nextHint = "   ⇐ next"
			}
			fmt.Printf("  │    %-20s  %s%s\n", tr.Trigger, command, nextHint)
		}
	}

	return nil
}

func runClusterIsolateV6Replicas(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	machine, err := state.NewMachine(clusterName)
	if err != nil {
		return err
	}

	sentinels, _ := dm.ListSentinels(ctx)

	if err := machine.Fire(ctx, state.TriggerBreakReplication, func(ctx context.Context) error {
		instances, err := dm.ListClusterRedisInstances(ctx, clusterName)
		if err != nil {
			return err
		}

		// Find current master port from sentinel.
		var masterPort int
		for _, s := range sentinels {
			info, err := redisclient.SentinelGetMaster(ctx, s.Port, clusterName)
			if err == nil {
				masterPort = info.Port
				break
			}
		}

		// Issue REPLICAOF NO ONE on each v6 instance that is NOT the current master.
		for _, ci := range instances {
			if ci.Version != 6 {
				continue
			}
			if ci.Port == masterPort {
				fmt.Printf("  skipping %s (current master)\n", ci.Name)
				continue
			}
			if err := redisclient.BreakReplication(ctx, ci.Port); err != nil {
				return err
			}
			fmt.Printf("  ✓ %s: REPLICAOF NO ONE\n", ci.Name)
		}

		// Reset all sentinels so they rediscover the live topology.
		for _, s := range sentinels {
			if err := redisclient.SentinelReset(ctx, s.Port, clusterName); err != nil {
				fmt.Fprintf(os.Stderr, "  warning: SENTINEL RESET on %s: %v\n", s.Name, err)
			} else {
				fmt.Printf("  ✓ sentinel %s: RESET %s\n", s.Name, clusterName)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	printNextUpgradeStep(ctx, clusterName)
	return nil
}

func runClusterMonitor(cmd *cobra.Command, args []string) error {
	clusterName := args[0]

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	if err := dm.Ping(ctx); err != nil {
		return fmt.Errorf("Docker not reachable: %w", err)
	}

	instances, err := dm.ListClusterRedisInstances(ctx, clusterName)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		return fmt.Errorf("cluster %q has no Redis instances", clusterName)
	}

	sentinels, err := dm.ListSentinels(ctx)
	if err != nil {
		return err
	}
	if len(sentinels) == 0 {
		return fmt.Errorf("no sentinels running")
	}
	sentinelAddrs := make([]string, 0, len(sentinels))
	for _, s := range sentinels {
		sentinelAddrs = append(sentinelAddrs, fmt.Sprintf("127.0.0.1:%d", s.Port))
	}

	// Look up the currently-promoted master for the header line. The
	// FailoverClient below will also resolve this independently and will
	// re-resolve if the master changes mid-stream.
	var masterPort int
	for _, s := range sentinels {
		if info, err := redisclient.SentinelGetMaster(ctx, s.Port, clusterName); err == nil {
			masterPort = info.Port
			break
		}
	}
	if masterPort == 0 {
		return fmt.Errorf("no sentinel knows the current master for %q", clusterName)
	}
	var masterName string
	for _, ci := range instances {
		if ci.Port == masterPort {
			masterName = ci.Name
			break
		}
	}
	fmt.Fprintf(os.Stderr, "Monitoring %s (current master per sentinel, host port %d). Ctrl-C to stop.\n", masterName, masterPort)

	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    clusterName,
		SentinelAddrs: sentinelAddrs,
		// russ runs on the host; sentinel reports masters by their russ-network
		// IP (e.g. 172.18.0.5:6380) which isn't routable from the host on
		// macOS Docker Desktop or Colima. Each russ container does publish its
		// port to 127.0.0.1:<same-port>, so rewrite whatever address go-redis
		// tries to dial to the host loopback alias.
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			_, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, network, "127.0.0.1:"+port)
		},
		// Disable the read deadline. go-redis defaults ReadTimeout to 3s and
		// applies it to every socket read inside the MONITOR goroutine. If the
		// consumer channel fills even briefly (terminal flush, OS hiccup), the
		// goroutine stalls trying to send, the deadline elapses while it's
		// stalled, and the next read errors and the stream silently dies. We
		// only use this client for MONITOR streaming, so there's no other
		// operation to protect with a timeout.
		ReadTimeout: -1,
	})
	defer rdb.Close()

	ch := make(chan string, 256)
	monitor := rdb.Monitor(ctx, ch)
	monitor.Start()
	defer monitor.Stop()
	if err := monitor.Err(); err != nil {
		return fmt.Errorf("start MONITOR: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			fmt.Println(msg)
		}
	}
}
