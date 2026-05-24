package cmd

import (
	"context"
	"fmt"

	"github.com/bigcommerce/russ/internal/docker"
	redisclient "github.com/bigcommerce/russ/internal/redis"
	"github.com/bigcommerce/russ/internal/state"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var instanceCmd = &cobra.Command{
	Use:   "instance",
	Short: "Manage individual Redis instances within a cluster",
}

var instanceAddCmd = &cobra.Command{
	Use:   "add <cluster-name>",
	Short: "Add a Redis instance to an existing cluster",
	Long: `Creates a new Redis replica and wires it into the named cluster.
The instance is configured as a replica of the current master (as reported
by sentinel). Sentinel auto-discovers the new replica via INFO REPLICATION.

If the cluster is currently all-v6 and --version=8 is supplied, the upgrade
lifecycle state advances to MixedVersions and the new v8 instance enters
with a deprioritized replica-priority (200) so a sentinel failover stays
within the v6 subset until the operator explicitly runs
'russ cluster promote-v8'. After promote-v8, additional v8 instances added
to the cluster enter at the promoted priority (1) to stay consistent with
the regime already in place.`,
	Args: cobra.ExactArgs(1),
	RunE: runInstanceAdd,
}

var instanceRmCmd = &cobra.Command{
	Use:   "rm <instance-name>",
	Short: "Remove a Redis instance from its cluster",
	Long: `Removes a replica instance: issues REPLICAOF NO ONE, resets all sentinels
so they forget the removed instance, then stops and removes the container.

The current master (as reported by sentinel) cannot be removed; run
'russ cluster failover' first to promote a different node.

After removing the last v6 instance, the upgrade state advances to V6Destroyed.`,
	Args: cobra.ExactArgs(1),
	RunE: runInstanceRm,
}

var instanceLsCmd = &cobra.Command{
	Use:   "ls <cluster-name>",
	Short: "List instances in a cluster",
	Args:  cobra.ExactArgs(1),
	RunE:  runInstanceLs,
}

func init() {
	instanceAddCmd.Flags().Int("version", 6, "Redis major version for the new instance: 6 or 8 (default 6)")
	instanceAddCmd.Flags().String("port-range", "6380-6500", "Port range for instance allocation")
	instanceAddCmd.Flags().String("max-memory", "256mb", "maxmemory limit for the new instance")
	instanceAddCmd.Flags().String("max-memory-policy", "volatile-lru", "Redis eviction policy when maxmemory is hit (volatile-lru, allkeys-lru, noeviction, etc.)")
	instanceAddCmd.Flags().Bool("enable-disk-persistence", true, "Enable AOF + RDB persistence and create a per-instance Docker volume mounted at /data. Default on. Pass --enable-disk-persistence=false if the existing cluster was created without persistence — russ doesn't currently auto-detect from existing instances, so the new instance must match the rest of the cluster.")

	instanceCmd.AddCommand(instanceAddCmd)
	instanceCmd.AddCommand(instanceRmCmd)
	instanceCmd.AddCommand(instanceLsCmd)
}

func runInstanceAdd(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	version, _ := cmd.Flags().GetInt("version")
	portRangeStr, _ := cmd.Flags().GetString("port-range")
	maxMemory, _ := cmd.Flags().GetString("max-memory")
	maxMemoryPolicy, _ := cmd.Flags().GetString("max-memory-policy")
	persistence, _ := cmd.Flags().GetBool("enable-disk-persistence")

	if version != 6 && version != 8 {
		return fmt.Errorf("--version must be 6 or 8")
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

	// Verify the cluster exists.
	instances, err := dm.ListClusterRedisInstances(ctx, clusterName)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		return fmt.Errorf("cluster %q not found; create it first with 'russ cluster create'", clusterName)
	}

	sentinels, err := dm.ListSentinels(ctx)
	if err != nil {
		return err
	}
	if len(sentinels) == 0 {
		return fmt.Errorf("no sentinels running; cannot determine current master")
	}

	// Ask sentinel for the current master.
	var masterInfo redisclient.MasterInfo
	var sentinelErrs []string
	for _, s := range sentinels {
		info, err := redisclient.SentinelGetMaster(ctx, s.Port, clusterName)
		if err == nil {
			masterInfo = info
			break
		}
		sentinelErrs = append(sentinelErrs, fmt.Sprintf("%s(:%d): %v", s.Name, s.Port, err))
	}
	if masterInfo.Port == 0 {
		log.Warn().Strs("sentinel_errors", sentinelErrs).Msg("no sentinel could report a master")
		return fmt.Errorf("no sentinel is monitoring cluster %q — run 'russ cluster create %s' first or check sentinel logs", clusterName, clusterName)
	}

	// Find the master container name (needed for REPLICAOF).
	var masterContainerName string
	for _, ci := range instances {
		if ci.Port == masterInfo.Port {
			masterContainerName = ci.Name
			break
		}
	}
	if masterContainerName == "" {
		return fmt.Errorf("sentinel reports master on port %d but no matching container found", masterInfo.Port)
	}

	if err := dm.PullImage(ctx, version); err != nil {
		return err
	}

	port, err := dm.AllocatePort(ctx, pr)
	if err != nil {
		return err
	}
	containerName := docker.InstanceContainerName(clusterName, port)

	// Pick the replica-priority for a new v8 entering the cluster based on
	// where the upgrade is in its lifecycle. During MixedVersions, v8 nodes
	// must be deprioritized so any sentinel failover stays within the v6
	// subset. After the operator runs `russ cluster promote-v8`, v8 nodes
	// enter at the promoted priority so the cluster's priority regime stays
	// internally consistent.
	priority := initialReplicaPriority(clusterName, version)

	id, err := dm.StartRedis(ctx, docker.StartRedisOpts{
		ContainerName:   containerName,
		Version:         version,
		HostPort:        port,
		Role:            docker.RoleReplica,
		ClusterName:     clusterName,
		MaxMemory:       maxMemory,
		MaxMemoryPolicy: maxMemoryPolicy,
		Persistence:     persistence,
		ReplicaPriority: priority,
	})
	if err != nil {
		return err
	}

	if err := redisclient.WaitForReady(ctx, port); err != nil {
		return dm.WrapWaitError(ctx, err, containerName)
	}

	// Capture the master's memory state immediately before the sync. The
	// hypothesis we're testing: each v8 replica add against a v6 master
	// leaves CoW pages from the BGSAVE fork retained until the parent
	// next writes to those pages. Under a low-write workload, those pages
	// accumulate across sequential adds, eventually pushing the master past
	// its container cap. If that's right, `used_memory_rss` will climb
	// monotonically across these snapshots while `used_memory` stays at
	// ~maxmemory.
	if pre, err := redisclient.GetMemoryUsage(ctx, masterInfo.Port); err == nil {
		log.Info().
			Int("master_port", masterInfo.Port).
			Int64("used_memory", pre.UsedMemory).
			Int64("used_memory_rss", pre.UsedMemoryRSS).
			Int64("used_memory_peak", pre.UsedMemoryPeak).
			Float64("frag_ratio", pre.MemFragmentationRatio).
			Msg("master memory before sync")
	}

	if err := redisclient.ConfigureReplica(ctx, port, masterContainerName, masterInfo.Port); err != nil {
		return err
	}
	if err := redisclient.WaitForReplication(ctx, port, masterInfo.Port); err != nil {
		return dm.WrapWaitError(ctx, err, containerName)
	}

	if post, err := redisclient.GetMemoryUsage(ctx, masterInfo.Port); err == nil {
		log.Info().
			Int("master_port", masterInfo.Port).
			Int64("used_memory", post.UsedMemory).
			Int64("used_memory_rss", post.UsedMemoryRSS).
			Int64("used_memory_peak", post.UsedMemoryPeak).
			Float64("frag_ratio", post.MemFragmentationRatio).
			Msg("master memory after sync")
	}

	log.Info().
		Str("name", containerName).
		Int("version", version).
		Int("port", port).
		Str("id", id[:12]).
		Str("master", fmt.Sprintf("%s:%d", masterContainerName, masterInfo.Port)).
		Msg("replica ready")

	// Force every sentinel to re-probe so the new replica isn't sitting in the
	// gap between info-refresh cycles, then wait until at least one sentinel
	// confirms the discovery. This closes the race where a failover triggered
	// immediately after `instance add` would strand the new replica in a chain.
	for _, s := range sentinels {
		_ = redisclient.SentinelReset(ctx, s.Port, clusterName)
	}
	if err := redisclient.WaitForSentinelToSeeReplica(ctx, sentinels[0].Port, clusterName, port); err != nil {
		log.Warn().Err(err).Msg("failovers may not include this replica until sentinel re-polls")
	} else {
		log.Info().Str("sentinel", sentinels[0].Name).Str("replica", containerName).Msg("sentinel sees replica")
	}

	// Advance the upgrade lifecycle state if applicable.
	currentState, _ := state.Load(clusterName)
	if version == 8 && currentState == state.StateAllV6 {
		machine, err := state.NewMachine(clusterName)
		if err != nil {
			log.Warn().Err(err).Msg("could not load state machine")
		} else {
			if err := machine.Fire(ctx, state.TriggerAddV8Instance, nil); err != nil {
				log.Warn().Err(err).Msg("state transition failed")
			} else {
				newState, _ := machine.Current(ctx)
				log.Info().Str("from", string(currentState)).Str("to", string(newState)).Msg("upgrade state advanced")
			}
		}
	}

	printNextUpgradeStep(ctx, clusterName)
	return nil
}

func runInstanceRm(cmd *cobra.Command, args []string) error {
	instanceName := args[0]
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	ci, err := dm.GetContainer(ctx, instanceName)
	if err != nil {
		return err
	}
	if ci.Role == docker.RoleSentinel {
		return fmt.Errorf("%s is a sentinel; use 'russ sentinel rm' instead", instanceName)
	}
	if ci.ClusterName == "" {
		return fmt.Errorf("container %s has no cluster label", instanceName)
	}

	clusterName := ci.ClusterName
	sentinels, _ := dm.ListSentinels(ctx)

	// Check if this instance is the current master.
	for _, s := range sentinels {
		info, err := redisclient.SentinelGetMaster(ctx, s.Port, clusterName)
		if err == nil && info.Port == ci.Port {
			return fmt.Errorf(
				"%s is the current master for cluster %q; run 'russ cluster failover %s' first",
				instanceName, clusterName, clusterName,
			)
		}
	}

	// Disconnect from replication.
	log.Info().Str("name", instanceName).Int("port", ci.Port).Msg("issuing REPLICAOF NO ONE")
	if err := redisclient.BreakReplication(ctx, ci.Port); err != nil {
		log.Warn().Err(err).Msg("REPLICAOF NO ONE failed; continuing with removal")
	}

	// Reset all sentinels so they forget this instance.
	for _, s := range sentinels {
		if err := redisclient.SentinelReset(ctx, s.Port, clusterName); err != nil {
			log.Warn().Err(err).Str("sentinel", s.Name).Msg("SENTINEL RESET")
		} else {
			log.Info().Str("sentinel", s.Name).Str("cluster", clusterName).Msg("SENTINEL RESET")
		}
	}

	if err := dm.StopAndRemove(ctx, ci.Name); err != nil {
		return err
	}
	log.Info().Str("name", instanceName).Msg("removed instance")

	// Check if the cluster has any v6 instances left; if not, advance state.
	remaining, err := dm.ListClusterRedisInstances(ctx, clusterName)
	if err != nil {
		return nil
	}
	hasV6 := false
	for _, r := range remaining {
		if r.Version == 6 {
			hasV6 = true
			break
		}
	}

	currentState, _ := state.Load(clusterName)
	if !hasV6 && currentState == state.StateReplicationBroken {
		machine, err := state.NewMachine(clusterName)
		if err == nil {
			if err := machine.Fire(ctx, state.TriggerDestroyV6, nil); err == nil {
				newState, _ := machine.Current(ctx)
				log.Info().Str("from", string(currentState)).Str("to", string(newState)).Msg("upgrade state advanced")
			}
		}
	}

	printNextUpgradeStep(ctx, clusterName)
	return nil
}

// initialReplicaPriority returns the replica-priority value a newly-added
// instance should boot with, based on the cluster's current upgrade-lifecycle
// state. Returns 0 (meaning "don't override; let Redis pick its default of
// 100") for v6 instances and for v8 instances added once the upgrade is
// effectively complete. The two non-default values match the regime
// established by `russ cluster promote-v8`.
func initialReplicaPriority(clusterName string, version int) int {
	if version != 8 {
		return 0
	}
	current, _ := state.Load(clusterName)
	switch current {
	case state.StateAllV6, state.StateMixedVersions:
		return redisclient.ReplicaPriorityV8Deprioritized
	default:
		// V8Prioritized, FailoverComplete, ReplicationBroken, V6Destroyed,
		// V8SentinelsAdded, Done — v8 nodes are the preferred candidates
		// from V8Prioritized onward; later states have no v6 left to
		// compete with anyway, but stay internally consistent.
		return redisclient.ReplicaPriorityV8Promoted
	}
}

func runInstanceLs(cmd *cobra.Command, args []string) error {
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
		fmt.Printf("No instances found for cluster %q.\n", clusterName)
		return nil
	}

	sentinels, _ := dm.ListSentinels(ctx)
	var sentinelMasterPort int
	for _, s := range sentinels {
		info, err := redisclient.SentinelGetMaster(ctx, s.Port, clusterName)
		if err == nil {
			sentinelMasterPort = info.Port
			break
		}
	}

	printInstanceTable(ctx, instances, sentinelMasterPort)
	return nil
}
