package cmd

import (
	"context"
	"fmt"
	"net"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/bigcommerce/russ/internal/docker"
	redisclient "github.com/bigcommerce/russ/internal/redis"
	"github.com/bigcommerce/russ/internal/state"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
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

var clusterFailoverCmd = &cobra.Command{
	Use:   "failover <cluster-name>",
	Short: "Trigger a sentinel-driven failover for a cluster",
	Long: `Pre-flight: walks every Redis instance and re-issues REPLICAOF against the
sentinel-reported master on any node that isn't already a direct replica
(chained replicas are invisible to sentinel and ineligible for promotion).
Then triggers SENTINEL FAILOVER and waits for the master port to change.

The winner is decided by replica-priority (lower = preferred). Set it
explicitly with 'russ cluster promote-v8' if you want to drive a
version-targeted failover; without that step, sentinel picks among
candidates with default priority (100).

Advances the upgrade lifecycle from V8Prioritized to FailoverComplete.
Safe to run from any state — outside of V8Prioritized the failover happens
but the FSM does not advance.`,
	Args: cobra.ExactArgs(1),
	RunE: runClusterFailover,
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

var clusterPromoteV8Cmd = &cobra.Command{
	Use:   "promote-v8 <cluster-name>",
	Short: "Swap v8 instances to a low replica-priority so the next failover picks v8",
	Long: `Sets replica-priority=1 on every v8 instance and replica-priority=100 on every
v6 instance, then waits until sentinel has observed the new values. After this
step, the next 'russ cluster failover <cluster>' will promote a v8 candidate,
breaking replication between v6 and v8.

Before promote-v8, v8 instances enter the cluster deprioritized (priority 200)
so a failover stays within the v6 subset and replication stays intact —
useful for exercising sentinel quorum behavior without committing to the
upgrade. This command is the explicit "OK, commit to the upgrade" step.

Advances the upgrade lifecycle state from MixedVersions to V8Prioritized.`,
	Args: cobra.ExactArgs(1),
	RunE: runClusterPromoteV8,
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
	clusterCreateCmd.Flags().String("max-memory", "256mb", "maxmemory limit applied to all instances")
	clusterCreateCmd.Flags().String("max-memory-policy", "volatile-lru", "Redis eviction policy when maxmemory is hit (volatile-lru, allkeys-lru, noeviction, etc.)")
	clusterCreateCmd.Flags().Bool("enable-disk-persistence", true, "Enable AOF + RDB persistence and create a per-instance Docker volume mounted at /data. Default on. Pass --enable-disk-persistence=false to disable; the dataset then lives entirely in RAM, bounded by --max-memory + eviction policy.")

	clusterCmd.AddCommand(clusterCreateCmd)
	clusterCmd.AddCommand(clusterLsCmd)
	clusterCmd.AddCommand(clusterRmCmd)
	clusterCmd.AddCommand(clusterStatusCmd)
	clusterCmd.AddCommand(clusterFailoverCmd)
	clusterCmd.AddCommand(clusterIsolateV6ReplicasCmd)
	clusterCmd.AddCommand(clusterPromoteV8Cmd)
	clusterCmd.AddCommand(clusterLifecycleCmd)
	clusterCmd.AddCommand(clusterMonitorCmd)
}

func runClusterCreate(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	version, _ := cmd.Flags().GetInt("version")
	count, _ := cmd.Flags().GetInt("count")
	portRangeStr, _ := cmd.Flags().GetString("port-range")
	maxMemory, _ := cmd.Flags().GetString("max-memory")
	maxMemoryPolicy, _ := cmd.Flags().GetString("max-memory-policy")
	persistence, _ := cmd.Flags().GetBool("enable-disk-persistence")

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

	log.Info().Str("cluster", clusterName).Int("count", count).Int("version", version).Str("maxmemory", maxMemory).Msg("creating cluster")

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
		MaxMemory:       maxMemory,
		MaxMemoryPolicy: maxMemoryPolicy,
		Persistence:     persistence,
	})
	if err != nil {
		return err
	}
	if err := redisclient.WaitForReady(ctx, masterPort); err != nil {
		return dm.WrapWaitError(ctx, err, masterName)
	}
	log.Info().Str("name", masterName).Int("port", masterPort).Str("id", masterID[:12]).Msg("master ready")

	// Start replicas and configure replication.
	for _, replicaPort := range ports[1:] {
		replicaName := docker.InstanceContainerName(clusterName, replicaPort)
		id, err := dm.StartRedis(ctx, docker.StartRedisOpts{
			ContainerName: replicaName,
			Version:       version,
			HostPort:      replicaPort,
			Role:          docker.RoleReplica,
			ClusterName:   clusterName,
			MaxMemory:       maxMemory,
			MaxMemoryPolicy: maxMemoryPolicy,
			Persistence:     persistence,
		})
		if err != nil {
			return err
		}
		if err := redisclient.WaitForReady(ctx, replicaPort); err != nil {
			return dm.WrapWaitError(ctx, err, replicaName)
		}
		if err := redisclient.ConfigureReplica(ctx, replicaPort, masterName, masterPort); err != nil {
			return err
		}
		if err := redisclient.WaitForReplication(ctx, replicaPort, masterPort); err != nil {
			return dm.WrapWaitError(ctx, err, replicaName)
		}
		log.Info().Str("name", replicaName).Int("port", replicaPort).Str("id", id[:12]).Msg("replica ready")
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
		log.Warn().Msg("no sentinels running. Run 'russ sentinel add' to add sentinels")
	} else {
		quorum := len(sentinels)/2 + 1
		log.Info().Str("cluster", clusterName).Int("sentinels", len(sentinels)).Int("quorum", quorum).Str("master", masterName).Str("master_ip", masterIP).Int("master_port", masterPort).Msg("registering cluster with sentinels")
		for _, s := range sentinels {
			if err := redisclient.SentinelMonitor(ctx, s.Port, clusterName, masterIP, masterPort, quorum); err != nil {
				return fmt.Errorf("SENTINEL MONITOR on %s: %w", s.Name, err)
			}
			log.Debug().Str("sentinel", s.Name).Msg("sentinel registered with cluster")
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
		log.Warn().Err(err).Msg("could not persist cluster state")
	}

	log.Info().Str("cluster", clusterName).Str("state", string(initialState)).Msg("cluster ready")
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
			log.Warn().Err(err).Str("sentinel", s.Name).Msg("SENTINEL REMOVE")
		} else {
			log.Info().Str("sentinel", s.Name).Str("cluster", clusterName).Msg("removed monitor")
		}
	}

	// Stop and remove all cluster containers.
	for _, ci := range instances {
		if err := dm.StopAndRemove(ctx, ci.Name); err != nil {
			log.Warn().Err(err).Str("name", ci.Name).Msg("remove container")
		} else {
			log.Info().Str("name", ci.Name).Msg("removed container")
		}
	}

	// Clean up persisted upgrade state.
	_ = state.Delete(clusterName)

	log.Info().Str("cluster", clusterName).Msg("cluster removed")
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
	state.TriggerPromoteV8:         "russ cluster promote-v8 <cluster>",
	state.TriggerFailover:          "russ cluster failover <cluster>",
	state.TriggerBreakReplication:  "russ cluster isolate-v6-replicas <cluster>",
	state.TriggerDestroyV6:         "russ instance rm <last v6 instance>   (auto-advances)",
	state.TriggerAddV8Sentinels:    "russ sentinel add --version=8",
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
				log.Debug().Str("name", ci.Name).Msg("skipping (current master)")
				continue
			}
			if err := redisclient.BreakReplication(ctx, ci.Port); err != nil {
				return err
			}
			log.Info().Str("name", ci.Name).Msg("REPLICAOF NO ONE")
		}

		// Reset all sentinels so they rediscover the live topology.
		for _, s := range sentinels {
			if err := redisclient.SentinelReset(ctx, s.Port, clusterName); err != nil {
				log.Warn().Err(err).Str("sentinel", s.Name).Msg("SENTINEL RESET")
			} else {
				log.Info().Str("sentinel", s.Name).Str("cluster", clusterName).Msg("SENTINEL RESET")
			}
		}
		return nil
	}); err != nil {
		return err
	}
	printNextUpgradeStep(ctx, clusterName)
	return nil
}

func runClusterFailover(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	sentinels, err := dm.ListSentinels(ctx)
	if err != nil {
		return err
	}
	if len(sentinels) == 0 {
		return fmt.Errorf("no sentinels running; cannot perform failover")
	}

	machine, err := state.NewMachine(clusterName)
	if err != nil {
		return err
	}

	doFailover := func(ctx context.Context) error {
		instances, err := dm.ListClusterRedisInstances(ctx, clusterName)
		if err != nil {
			return err
		}

		// Pre-flight reconciliation: ensure every replica points directly at the
		// sentinel-reported master. A chained replica (e.g. one added between the
		// previous failover and sentinel's next info-refresh) is invisible to
		// sentinel and ineligible for promotion, so a v8-targeted failover would
		// hang waiting for a node sentinel can't see.
		if err := reconcileReplication(ctx, dm, sentinels, clusterName, instances); err != nil {
			return fmt.Errorf("pre-failover reconciliation: %w", err)
		}

		// Identify the current master from sentinel's live view rather than from
		// container labels. The russ.role label is a creation-time snapshot
		// and goes stale after any prior failover — using it would cause us
		// to set priorities on the wrong node and wait for sentinel to report
		// the actual master as a replica (which it never will).
		var currentMasterPort int
		for _, s := range sentinels {
			if info, err := redisclient.SentinelGetMaster(ctx, s.Port, clusterName); err == nil {
				currentMasterPort = info.Port
				break
			}
		}
		if currentMasterPort == 0 {
			return fmt.Errorf("could not determine current master from any sentinel")
		}

		// Collect every candidate port (anything that isn't the current master)
		// so we can wait for the master to switch to one of them. Which port
		// actually wins is decided by sentinel against the replica-priority
		// regime set up by the operator — 'russ cluster promote-v8' is the
		// command that biases the choice toward v8. The failover step itself
		// stays version-agnostic.
		candidatePorts := make([]int, 0, len(instances))
		for _, ci := range instances {
			if ci.Port != currentMasterPort {
				candidatePorts = append(candidatePorts, ci.Port)
			}
		}

		log.Info().Str("cluster", clusterName).Str("via", sentinels[0].Name).Msg("triggering SENTINEL FAILOVER")
		// Sentinel can return NOGOODSLAVE briefly after a topology change while
		// it's still polling INFO from freshly-discovered replicas. Retry with
		// backoff so the operator doesn't have to time it manually.
		var failoverErr error
		for attempt := 1; attempt <= 6; attempt++ {
			failoverErr = redisclient.SentinelFailover(ctx, sentinels[0].Port, clusterName)
			if failoverErr == nil {
				break
			}
			if !strings.Contains(failoverErr.Error(), "NOGOODSLAVE") {
				return failoverErr
			}
			log.Warn().Err(failoverErr).Int("attempt", attempt).Msg("sentinel not ready; retrying in 5s")
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
		if failoverErr != nil {
			return failoverErr
		}

		if len(candidatePorts) > 0 {
			log.Info().Ints("candidate_ports", candidatePorts).Msg("waiting for master to switch")
			promoted, err := redisclient.WaitForFailover(ctx, sentinels[0].Port, clusterName, candidatePorts...)
			if err != nil {
				return err
			}
			log.Info().Int("port", promoted).Msg("master switched")
			return nil
		}

		log.Info().Msg("failover triggered; sentinel will elect a new master")
		return nil
	}

	// Failover is also a general-purpose operation, not exclusively an upgrade
	// step. If the FSM permits a Failover trigger from the current state
	// (currently only V8Prioritized), advance the state machine after the
	// failover completes. Otherwise just run the failover and leave the
	// lifecycle state alone.
	if ok, _ := machine.CanFire(ctx, state.TriggerFailover); ok {
		if err := machine.Fire(ctx, state.TriggerFailover, doFailover); err != nil {
			return err
		}
		printNextUpgradeStep(ctx, clusterName)
		return nil
	}
	current, _ := machine.Current(ctx)
	log.Info().Str("state", string(current)).Msg("cluster is not in a state where failover advances the FSM; failover will run but upgrade state will not advance")
	if err := doFailover(ctx); err != nil {
		return err
	}
	printNextUpgradeStep(ctx, clusterName)
	return nil
}

func runClusterPromoteV8(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	sentinels, err := dm.ListSentinels(ctx)
	if err != nil {
		return err
	}
	if len(sentinels) == 0 {
		return fmt.Errorf("no sentinels running; cannot verify priority propagation")
	}

	machine, err := state.NewMachine(clusterName)
	if err != nil {
		return err
	}

	if err := machine.Fire(ctx, state.TriggerPromoteV8, func(ctx context.Context) error {
		instances, err := dm.ListClusterRedisInstances(ctx, clusterName)
		if err != nil {
			return err
		}

		// Identify the current master from sentinel's live view, not from
		// container labels — see runClusterFailover for the same reasoning.
		var currentMasterPort int
		for _, s := range sentinels {
			if info, err := redisclient.SentinelGetMaster(ctx, s.Port, clusterName); err == nil {
				currentMasterPort = info.Port
				break
			}
		}
		if currentMasterPort == 0 {
			return fmt.Errorf("could not determine current master from any sentinel")
		}

		var v6Instances, v8Instances []docker.ContainerInfo
		for _, ci := range instances {
			if ci.Version == 6 {
				v6Instances = append(v6Instances, ci)
			} else {
				v8Instances = append(v8Instances, ci)
			}
		}
		if len(v8Instances) == 0 {
			return fmt.Errorf("cluster %q has no v8 instances to promote — add some with 'russ instance add %s --version=8' first", clusterName, clusterName)
		}

		log.Info().
			Int("v8_count", len(v8Instances)).
			Int("v6_count", len(v6Instances)).
			Int("v8_priority", redisclient.ReplicaPriorityV8Promoted).
			Int("v6_priority", redisclient.ReplicaPriorityDefault).
			Msg("swapping replica priorities so the next failover picks a v8")

		expectedPriorities := map[int]int{}
		for _, ci := range v8Instances {
			if ci.Port == currentMasterPort {
				continue
			}
			if err := redisclient.SetReplicaPriority(ctx, ci.Port, redisclient.ReplicaPriorityV8Promoted); err != nil {
				return err
			}
			log.Debug().Str("instance", ci.Name).Int("priority", redisclient.ReplicaPriorityV8Promoted).Msg("v8 replica-priority set")
			expectedPriorities[ci.Port] = redisclient.ReplicaPriorityV8Promoted
		}
		for _, ci := range v6Instances {
			if ci.Port == currentMasterPort {
				continue
			}
			if err := redisclient.SetReplicaPriority(ctx, ci.Port, redisclient.ReplicaPriorityDefault); err != nil {
				return err
			}
			log.Debug().Str("instance", ci.Name).Int("priority", redisclient.ReplicaPriorityDefault).Msg("v6 replica-priority set")
			expectedPriorities[ci.Port] = redisclient.ReplicaPriorityDefault
		}

		// CONFIG SET takes effect immediately on the replica, but sentinel's
		// view of replica-priority comes from its periodic INFO REPLICATION
		// poll. Wait until sentinel has actually observed the new values
		// before declaring the step complete — otherwise a follow-up
		// 'cluster failover' that races ahead of the next poll would still
		// pick a v6.
		log.Info().Msg("waiting for sentinel to observe the new replica priorities")
		if err := redisclient.WaitForReplicaPriorities(ctx, sentinels[0].Port, clusterName, expectedPriorities, 30*time.Second); err != nil {
			return fmt.Errorf("priority propagation: %w", err)
		}
		log.Info().Msg("sentinel sees the new priorities; v8 will win the next failover")
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
	log.Info().Str("master", masterName).Int("port", masterPort).Msg("monitoring current master (Ctrl-C to stop)")

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
