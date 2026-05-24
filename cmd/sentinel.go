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

var sentinelCmd = &cobra.Command{
	Use:   "sentinel",
	Short: "Manage Redis Sentinel instances",
}

var sentinelBootstrapCmd = &cobra.Command{
	Use:     "add",
	Aliases: []string{"bootstrap"},
	Short:   "Add Redis Sentinel instances to the russ fleet",
	Long: `Creates N Redis Sentinel containers on the russ Docker network. Use the
first time to create the fleet, and again later (e.g. with --version=8) to add
new sentinels alongside the existing ones.

When started with --version=8 against clusters in the V6Destroyed state, the
new sentinels are registered to monitor each cluster, the fleet-wide quorum is
recalculated, and the lifecycle state advances to V8SentinelsAdded.

Sentinels start with no monitored masters; masters are registered automatically
when clusters are created with 'russ cluster create'.

Instance count should be an odd number (1, 3, 5) to ensure quorum.`,
	RunE: runSentinelBootstrap,
}

var sentinelLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List running sentinel instances",
	RunE:  runSentinelLs,
}

var sentinelRmCmd = &cobra.Command{
	Use:   "rm <container-name>",
	Short: "Remove a sentinel instance",
	Args:  cobra.ExactArgs(1),
	RunE:  runSentinelRm,
}

func init() {
	sentinelBootstrapCmd.Flags().Int("version", 6, "Redis major version: 6 or 8 (default 6)")
	sentinelBootstrapCmd.Flags().Int("count", 3, "Number of sentinel instances to create (odd number recommended)")
	sentinelBootstrapCmd.Flags().String("port-range", "26379-26450", "Port range for sentinel allocation")
	sentinelBootstrapCmd.Flags().String("redis-source-version", "", "Exact Redis source version to compile into the sentinel image (e.g. 6.2.17, 8.6.3); defaults by major version")
	sentinelBootstrapCmd.Flags().Bool("force-rebuild", false, "Force rebuild of the sentinel image even if it already exists")

	sentinelCmd.AddCommand(sentinelBootstrapCmd)
	sentinelCmd.AddCommand(sentinelLsCmd)
	sentinelCmd.AddCommand(sentinelRmCmd)
}

func runSentinelBootstrap(cmd *cobra.Command, _ []string) error {
	version, _ := cmd.Flags().GetInt("version")
	count, _ := cmd.Flags().GetInt("count")
	portRangeStr, _ := cmd.Flags().GetString("port-range")
	redisSourceVersion, _ := cmd.Flags().GetString("redis-source-version")
	forceRebuild, _ := cmd.Flags().GetBool("force-rebuild")

	if version != 6 && version != 8 {
		return fmt.Errorf("--version must be 6 or 8")
	}
	if count < 1 {
		return fmt.Errorf("--count must be at least 1")
	}
	if count%2 == 0 {
		log.Warn().Int("count", count).Msg("even sentinel count has no clear quorum majority; odd numbers (1, 3, 5) are recommended")
	}

	// Resolve the Redis source version to compile into the sentinel image.
	if redisSourceVersion == "" {
		redisSourceVersion = docker.DefaultRedisSourceVersion[version]
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

	if version == 8 {
		clusters, _ := state.ListClusters()
		var notReady []string
		for _, name := range clusters {
			s, _ := state.Load(name)
			if s != state.StateV6Destroyed && s != state.StateDone {
				notReady = append(notReady, fmt.Sprintf("%s (%s)", name, s))
			}
		}
		if len(notReady) > 0 {
			log.Warn().Strs("clusters", notReady).
				Msg("the following clusters are not yet in V6Destroyed and will NOT be registered with the new v8 sentinels; finish upgrading them first, then re-run 'sentinel add --version=8'")
		}
	}

	if err := dm.EnsureNetwork(ctx); err != nil {
		return err
	}

	// Build (or verify) the tilt-patched sentinel image before starting any containers.
	if err := dm.BuildSentinelImage(ctx, version, redisSourceVersion, forceRebuild); err != nil {
		return fmt.Errorf("build sentinel image: %w", err)
	}

	// Snapshot existing sentinels before adding new ones so we can distinguish
	// them from the newly-started ones for cluster registration.
	priorSentinels, _ := dm.ListSentinels(ctx)

	log.Info().Int("count", count).Int("version", version).Msg("bootstrapping sentinels")

	newPorts := make([]int, 0, count)
	for i := 0; i < count; i++ {
		port, err := dm.AllocatePort(ctx, pr)
		if err != nil {
			return fmt.Errorf("allocate port for sentinel %d: %w", i+1, err)
		}
		name := docker.SentinelContainerName(port)
		id, err := dm.StartSentinel(ctx, docker.StartSentinelOpts{
			ContainerName: name,
			Version:       version,
			HostPort:      port,
		})
		if err != nil {
			return err
		}
		if err := redisclient.WaitForReady(ctx, port); err != nil {
			return dm.WrapWaitError(ctx, fmt.Errorf("sentinel %s did not become ready: %w", name, err), name)
		}
		log.Info().Str("name", name).Int("port", port).Str("id", id[:12]).Msg("sentinel ready")
		newPorts = append(newPorts, port)
	}

	allSentinels, _ := dm.ListSentinels(ctx)
	log.Info().Int("total", len(allSentinels)).Msg("sentinel fleet ready")

	if version == 8 {
		registerClustersWithNewSentinels(ctx, priorSentinels, newPorts, allSentinels)
		advanceClustersInState(ctx, state.StateV6Destroyed, state.TriggerAddV8Sentinels)
	}
	return nil
}

func runSentinelLs(cmd *cobra.Command, _ []string) error {
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
	docker.PrintContainerTable(sentinels)
	return nil
}

// registerClustersWithNewSentinels registers every V6Destroyed cluster with the newly-started
// v8 sentinels (newPorts) and updates the quorum on all sentinels (fleet-wide).
// priorSentinels are queried for master info; allSentinels is the full post-bootstrap fleet.
func registerClustersWithNewSentinels(ctx context.Context, priorSentinels []docker.ContainerInfo, newPorts []int, allSentinels []docker.ContainerInfo) {
	if len(priorSentinels) == 0 || len(newPorts) == 0 {
		if len(priorSentinels) == 0 {
			clusters, _ := state.ListClusters()
			for _, name := range clusters {
				if s, _ := state.Load(name); s == state.StateV6Destroyed {
					log.Warn().Str("cluster", name).
						Msg("cluster is in V6Destroyed but has no prior sentinels to query for master info — skipping registration. Ensure v6 sentinels are running before bootstrapping v8 sentinels, or register manually via SENTINEL MONITOR")
				}
			}
		}
		return
	}
	quorum := len(allSentinels)/2 + 1

	clusters, err := state.ListClusters()
	if err != nil {
		log.Warn().Err(err).Msg("list clusters for sentinel registration")
		return
	}

	for _, clusterName := range clusters {
		s, _ := state.Load(clusterName)
		if s != state.StateV6Destroyed {
			continue
		}

		// Get current master from an existing sentinel.
		var masterInfo redisclient.MasterInfo
		var found bool
		for _, ps := range priorSentinels {
			info, err := redisclient.SentinelGetMaster(ctx, ps.Port, clusterName)
			if err == nil {
				masterInfo = info
				found = true
				break
			}
		}
		if !found {
			log.Warn().Str("cluster", clusterName).Msg("no existing sentinel has master info; skipping registration")
			continue
		}

		// Register each new sentinel with this cluster.
		for _, port := range newPorts {
			if err := redisclient.SentinelMonitor(ctx, port, clusterName, masterInfo.Host, masterInfo.Port, quorum); err != nil {
				log.Warn().Err(err).Str("cluster", clusterName).Int("sentinel_port", port).Msg("register cluster with new sentinel")
			} else {
				log.Info().Int("sentinel_port", port).Str("cluster", clusterName).Int("master_port", masterInfo.Port).Int("quorum", quorum).Msg("sentinel monitoring cluster")
			}
		}

		// Update quorum on the pre-existing sentinels; new ones received it via SentinelMonitor.
		for _, ps := range priorSentinels {
			if err := redisclient.SentinelSetQuorum(ctx, ps.Port, clusterName, quorum); err != nil {
				log.Warn().Err(err).Int("sentinel_port", ps.Port).Str("cluster", clusterName).Msg("update quorum on sentinel")
			}
		}
		log.Info().Int("quorum", quorum).Str("cluster", clusterName).Int("sentinels", len(allSentinels)).Msg("quorum updated")
	}
}

// updateQuorumAcrossSentinels recalculates and pushes the correct quorum to every
// sentinel in the fleet for every cluster any of them monitors. Call this after
// adding or removing sentinels.
func updateQuorumAcrossSentinels(ctx context.Context, sentinels []docker.ContainerInfo) {
	quorum := len(sentinels)/2 + 1
	clusterNames, err := redisclient.SentinelListMasters(ctx, sentinels[0].Port)
	if err != nil {
		log.Warn().Err(err).Msg("list monitored clusters for quorum update")
		return
	}
	for _, clusterName := range clusterNames {
		for _, s := range sentinels {
			if err := redisclient.SentinelSetQuorum(ctx, s.Port, clusterName, quorum); err != nil {
				log.Warn().Err(err).Int("sentinel_port", s.Port).Str("cluster", clusterName).Msg("update quorum on sentinel")
			}
		}
		log.Info().Int("quorum", quorum).Str("cluster", clusterName).Int("sentinels_remaining", len(sentinels)).Msg("quorum updated")
	}
}

// reconcileReplication walks every Redis instance in cluster instances and ensures
// each non-master is replicating directly from the sentinel-reported master.
// Idempotent: nodes already pointing at the master are left alone.
//
// This is the antidote to chained replication: if a replica was added between
// sentinel info-refresh cycles and a failover happened in that window, the new
// replica gets stranded pointing at a former master (now itself a replica),
// invisible to sentinel and ineligible for future promotion.
func reconcileReplication(ctx context.Context, dm *docker.Manager, sentinels []docker.ContainerInfo, clusterName string, instances []docker.ContainerInfo) error {
	var masterInfo redisclient.MasterInfo
	var found bool
	for _, s := range sentinels {
		info, err := redisclient.SentinelGetMaster(ctx, s.Port, clusterName)
		if err == nil {
			masterInfo = info
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("no sentinel could report a master for %q", clusterName)
	}

	var masterContainerName string
	for _, ci := range instances {
		if ci.Port == masterInfo.Port {
			masterContainerName = ci.Name
			break
		}
	}
	if masterContainerName == "" {
		return fmt.Errorf("sentinel reports master on port %d but no russ container matches", masterInfo.Port)
	}

	rewired := false
	for _, ci := range instances {
		if ci.Port == masterInfo.Port {
			continue
		}
		_, currentPort, err := redisclient.ReplicationTarget(ctx, ci.Port)
		if err != nil {
			log.Warn().Err(err).Str("instance", ci.Name).Msg("could not read replication state")
			continue
		}
		if currentPort == masterInfo.Port {
			continue // already a direct replica of the master
		}
		log.Info().Str("instance", ci.Name).Int("from_port", currentPort).Str("master", masterContainerName).Int("master_port", masterInfo.Port).Msg("rewiring replica")
		if err := redisclient.ConfigureReplica(ctx, ci.Port, masterContainerName, masterInfo.Port); err != nil {
			return fmt.Errorf("REPLICAOF on %s: %w", ci.Name, err)
		}
		if err := redisclient.WaitForReplication(ctx, ci.Port, masterInfo.Port); err != nil {
			return fmt.Errorf("waiting for %s to sync: %w", ci.Name, err)
		}
		rewired = true
	}

	if rewired {
		// Wait for sentinel to natural-discover the rewired replicas via its
		// periodic INFO REPLICATION poll on the master (~10s cycle). Don't
		// SENTINEL RESET here — that would wipe sentinel's known-good state for
		// replicas that didn't need rewiring and put the whole fleet back into
		// "not yet pinged" purgatory just as we're about to fail over.
		log.Info().Msg("waiting for sentinel to discover rewired replicas")
		for _, ci := range instances {
			if ci.Port == masterInfo.Port {
				continue
			}
			if err := redisclient.WaitForSentinelToSeeReplica(ctx, sentinels[0].Port, clusterName, ci.Port); err != nil {
				return fmt.Errorf("after reconciliation: %w", err)
			}
		}
		log.Info().Msg("all replicas visible to sentinel")
	}
	return nil
}

// advanceClustersInState fires trigger on every cluster currently in fromState.
func advanceClustersInState(ctx context.Context, fromState state.State, trigger string) {
	clusters, err := state.ListClusters()
	if err != nil {
		log.Warn().Err(err).Msg("could not list clusters")
		return
	}
	for _, clusterName := range clusters {
		current, _ := state.Load(clusterName)
		if current != fromState {
			continue
		}
		machine, err := state.NewMachine(clusterName)
		if err != nil {
			log.Warn().Err(err).Str("cluster", clusterName).Msg("state machine")
			continue
		}
		if err := machine.Fire(ctx, trigger, nil); err != nil {
			log.Warn().Err(err).Str("cluster", clusterName).Msg("advance state")
			continue
		}
		newState, _ := machine.Current(ctx)
		log.Info().Str("cluster", clusterName).Str("from", string(fromState)).Str("to", string(newState)).Msg("upgrade state advanced")
		printNextUpgradeStep(ctx, clusterName)
	}
}

func runSentinelRm(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	ci, err := dm.GetContainer(ctx, args[0])
	if err != nil {
		return err
	}
	if ci.Role != docker.RoleSentinel {
		return fmt.Errorf("%s is not a sentinel (role=%s); use 'russ instance rm' for Redis instances", args[0], ci.Role)
	}

	// Before removing, tell the sentinel to stop monitoring all clusters it knows about.
	masters, err := redisclient.SentinelListMasters(ctx, ci.Port)
	if err != nil {
		log.Warn().Err(err).Str("sentinel", ci.Name).Msg("could not list masters on sentinel")
	}
	for _, m := range masters {
		if err := redisclient.SentinelRemove(ctx, ci.Port, m); err != nil {
			log.Warn().Err(err).Str("master", m).Str("sentinel", ci.Name).Msg("SENTINEL REMOVE")
		}
	}

	if err := dm.StopAndRemove(ctx, ci.Name); err != nil {
		return err
	}
	log.Info().Str("name", ci.Name).Msg("removed sentinel")

	remaining, _ := dm.ListSentinels(ctx)
	if len(remaining) > 0 {
		updateQuorumAcrossSentinels(ctx, remaining)
	}

	if ci.Version == 6 {
		hasV6 := false
		for _, s := range remaining {
			if s.Version == 6 {
				hasV6 = true
				break
			}
		}
		if !hasV6 {
			advanceClustersInState(ctx, state.StateV8SentinelsAdded, state.TriggerRemoveV6Sentinels)
		}
	}
	return nil
}
