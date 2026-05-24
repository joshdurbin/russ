package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bigcommerce/russ/internal/docker"
	redisclient "github.com/bigcommerce/russ/internal/redis"
	"github.com/bigcommerce/russ/internal/state"
	"github.com/spf13/cobra"
)

var sentinelCmd = &cobra.Command{
	Use:   "sentinel",
	Short: "Manage Redis Sentinel instances",
}

var sentinelBootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Bootstrap a fleet of Redis Sentinel instances",
	Long: `Creates N Redis Sentinel containers on the russ Docker network.
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

var sentinelFailoverCmd = &cobra.Command{
	Use:   "failover <cluster-name>",
	Short: "Trigger a sentinel failover for a cluster",
	Long: `In mixed-version clusters (v6+v8), sets replica-priority 1 on v8 instances
and 100 on v6 instances to prefer v8 promotion, then triggers SENTINEL FAILOVER.
In all-v6 clusters, triggers an unconditional failover.

Advances the upgrade lifecycle state to FailoverComplete.`,
	Args: cobra.ExactArgs(1),
	RunE: runSentinelFailover,
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
	sentinelCmd.AddCommand(sentinelFailoverCmd)
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
		fmt.Fprintf(os.Stderr, "warning: even sentinel count (%d) has no clear quorum majority; odd numbers (1, 3, 5) are recommended\n", count)
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
			fmt.Fprintln(os.Stderr, "Warning: the following clusters are not yet in V6Destroyed and will NOT be registered with the new v8 sentinels:")
			for _, n := range notReady {
				fmt.Fprintf(os.Stderr, "  • %s\n", n)
			}
			fmt.Fprintln(os.Stderr, "Finish upgrading those clusters to V6Destroyed first, then re-run 'sentinel bootstrap --version=8'.")
			fmt.Fprintln(os.Stderr)
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

	fmt.Printf("Bootstrapping %d Redis %d.x sentinel(s)...\n", count, version)

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
			return fmt.Errorf("sentinel %s did not become ready: %w", name, err)
		}
		fmt.Printf("  ✓ %s  port=%d  id=%.12s\n", name, port, id)
		newPorts = append(newPorts, port)
	}

	allSentinels, _ := dm.ListSentinels(ctx)
	fmt.Printf("\nSentinel fleet: %d instance(s) ready.\n", len(allSentinels))

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
					fmt.Fprintf(os.Stderr, "Warning: cluster %q is in V6Destroyed but has no prior sentinels to query for master info — skipping registration.\n", name)
					fmt.Fprintln(os.Stderr, "  Ensure v6 sentinels are running before bootstrapping v8 sentinels, or register manually via SENTINEL MONITOR.")
				}
			}
		}
		return
	}
	quorum := len(allSentinels)/2 + 1

	clusters, err := state.ListClusters()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: list clusters for sentinel registration: %v\n", err)
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
			fmt.Fprintf(os.Stderr, "  warning: no existing sentinel has master info for cluster %q; skipping registration\n", clusterName)
			continue
		}

		// Register each new sentinel with this cluster.
		for _, port := range newPorts {
			if err := redisclient.SentinelMonitor(ctx, port, clusterName, masterInfo.Host, masterInfo.Port, quorum); err != nil {
				fmt.Fprintf(os.Stderr, "  warning: register %q with new sentinel :%d: %v\n", clusterName, port, err)
			} else {
				fmt.Printf("  ✓ sentinel :%d monitoring cluster %q (master :%d, quorum=%d)\n", port, clusterName, masterInfo.Port, quorum)
			}
		}

		// Update quorum on the pre-existing sentinels; new ones received it via SentinelMonitor.
		for _, ps := range priorSentinels {
			if err := redisclient.SentinelSetQuorum(ctx, ps.Port, clusterName, quorum); err != nil {
				fmt.Fprintf(os.Stderr, "  warning: update quorum on sentinel :%d for %q: %v\n", ps.Port, clusterName, err)
			}
		}
		fmt.Printf("  Quorum updated to %d for cluster %q (%d sentinel(s) total)\n", quorum, clusterName, len(allSentinels))
	}
}

// updateQuorumAcrossSentinels recalculates and pushes the correct quorum to every
// sentinel in the fleet for every cluster any of them monitors. Call this after
// adding or removing sentinels.
func updateQuorumAcrossSentinels(ctx context.Context, sentinels []docker.ContainerInfo) {
	quorum := len(sentinels)/2 + 1
	clusterNames, err := redisclient.SentinelListMasters(ctx, sentinels[0].Port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  warning: list monitored clusters for quorum update: %v\n", err)
		return
	}
	for _, clusterName := range clusterNames {
		for _, s := range sentinels {
			if err := redisclient.SentinelSetQuorum(ctx, s.Port, clusterName, quorum); err != nil {
				fmt.Fprintf(os.Stderr, "  warning: update quorum on sentinel :%d for %q: %v\n", s.Port, clusterName, err)
			}
		}
		fmt.Printf("  Quorum updated to %d for cluster %q (%d sentinel(s) remaining)\n", quorum, clusterName, len(sentinels))
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
			fmt.Fprintf(os.Stderr, "  warning: could not read replication state of %s: %v\n", ci.Name, err)
			continue
		}
		if currentPort == masterInfo.Port {
			continue // already a direct replica of the master
		}
		fmt.Printf("  rewiring %s: replicating from port %d → %s (port %d)\n",
			ci.Name, currentPort, masterContainerName, masterInfo.Port)
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
		fmt.Println("  waiting for sentinel to discover rewired replicas...")
		for _, ci := range instances {
			if ci.Port == masterInfo.Port {
				continue
			}
			if err := redisclient.WaitForSentinelToSeeReplica(ctx, sentinels[0].Port, clusterName, ci.Port); err != nil {
				return fmt.Errorf("after reconciliation: %w", err)
			}
		}
		fmt.Println("  ✓ all replicas visible to sentinel")
	}
	return nil
}

// advanceClustersInState fires trigger on every cluster currently in fromState.
func advanceClustersInState(ctx context.Context, fromState state.State, trigger string) {
	clusters, err := state.ListClusters()
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not list clusters: %v\n", err)
		return
	}
	for _, clusterName := range clusters {
		current, _ := state.Load(clusterName)
		if current != fromState {
			continue
		}
		machine, err := state.NewMachine(clusterName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: state machine for %s: %v\n", clusterName, err)
			continue
		}
		if err := machine.Fire(ctx, trigger, nil); err != nil {
			fmt.Fprintf(os.Stderr, "warning: advance state for cluster %q: %v\n", clusterName, err)
			continue
		}
		newState, _ := machine.Current(ctx)
		fmt.Printf("Upgrade state for cluster %q: %s → %s\n", clusterName, fromState, newState)
		printNextUpgradeStep(ctx, clusterName)
	}
}

func runSentinelFailover(cmd *cobra.Command, args []string) error {
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

		var v6Instances, v8Instances []docker.ContainerInfo
		for _, ci := range instances {
			if ci.Version == 6 {
				v6Instances = append(v6Instances, ci)
			} else {
				v8Instances = append(v8Instances, ci)
			}
		}

		expectedPriorities := map[int]int{}
		if len(v8Instances) > 0 && len(v6Instances) > 0 {
			fmt.Println("Setting replica priorities (v8=1, v6=100)...")
			for _, ci := range v8Instances {
				if ci.Port == currentMasterPort {
					continue
				}
				if err := redisclient.SetReplicaPriority(ctx, ci.Port, 1); err != nil {
					return err
				}
				fmt.Printf("  ✓ %s (v8) replica-priority=1\n", ci.Name)
				expectedPriorities[ci.Port] = 1
			}
			for _, ci := range v6Instances {
				if ci.Port == currentMasterPort {
					continue
				}
				if err := redisclient.SetReplicaPriority(ctx, ci.Port, 100); err != nil {
					return err
				}
				fmt.Printf("  ✓ %s (v6) replica-priority=100\n", ci.Name)
				expectedPriorities[ci.Port] = 100
			}

			// CONFIG SET takes effect immediately on the replica, but sentinel's
			// view of replica-priority comes from its periodic INFO REPLICATION
			// poll (~10s default). If we trigger SENTINEL FAILOVER before that
			// next poll, sentinel still sees the default priority of 100 across
			// the board and picks the winner by offset/runid — often landing on
			// a v6 instead of our intended v8. Wait until sentinel has actually
			// observed the new values.
			fmt.Println("Waiting for sentinel to observe new replica priorities...")
			if err := redisclient.WaitForReplicaPriorities(ctx, sentinels[0].Port, clusterName, expectedPriorities, 30*time.Second); err != nil {
				return fmt.Errorf("priority propagation: %w", err)
			}
			fmt.Println("  ✓ sentinel sees the updated priorities")
		}

		// Collect all candidate v8 ports — sentinel picks among them by offset/runid
		// once priorities are equal, so we accept any of them as a successful target.
		v8Ports := make([]int, 0, len(v8Instances))
		for _, ci := range v8Instances {
			if ci.Port != currentMasterPort {
				v8Ports = append(v8Ports, ci.Port)
			}
		}

		fmt.Printf("Triggering SENTINEL FAILOVER %q via %s...\n", clusterName, sentinels[0].Name)
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
			fmt.Printf("  sentinel not ready (attempt %d/6: %v); waiting 5s...\n", attempt, failoverErr)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
			}
		}
		if failoverErr != nil {
			return failoverErr
		}

		if len(v8Ports) > 0 {
			fmt.Printf("Waiting for master to switch to a v8 instance (ports %v)...\n", v8Ports)
			promoted, err := redisclient.WaitForFailover(ctx, sentinels[0].Port, clusterName, v8Ports...)
			if err != nil {
				return err
			}
			fmt.Printf("  ✓ master is now port %d\n", promoted)
			return nil
		}

		fmt.Println("Failover triggered; sentinel will elect a new master.")
		return nil
	}

	// SENTINEL FAILOVER is also a general-purpose operation, not exclusively an
	// upgrade step. If the FSM permits a Failover trigger from the current state
	// (currently only MixedVersions), advance the state machine after the failover
	// completes. Otherwise just run the failover and leave the lifecycle state alone.
	if ok, _ := machine.CanFire(ctx, state.TriggerFailover); ok {
		if err := machine.Fire(ctx, state.TriggerFailover, doFailover); err != nil {
			return err
		}
		printNextUpgradeStep(ctx, clusterName)
		return nil
	}
	current, _ := machine.Current(ctx)
	fmt.Printf("Note: cluster is in state %s; failover will run but upgrade state will not advance.\n", current)
	if err := doFailover(ctx); err != nil {
		return err
	}
	printNextUpgradeStep(ctx, clusterName)
	return nil
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
		fmt.Fprintf(os.Stderr, "warning: could not list masters on sentinel %s: %v\n", ci.Name, err)
	}
	for _, m := range masters {
		if err := redisclient.SentinelRemove(ctx, ci.Port, m); err != nil {
			fmt.Fprintf(os.Stderr, "warning: SENTINEL REMOVE %s on %s: %v\n", m, ci.Name, err)
		}
	}

	if err := dm.StopAndRemove(ctx, ci.Name); err != nil {
		return err
	}
	fmt.Printf("Removed sentinel %s\n", ci.Name)

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
