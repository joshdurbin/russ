package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/bigcommerce/russ/internal/docker"
	redisclient "github.com/bigcommerce/russ/internal/redis"
	"github.com/bigcommerce/russ/internal/state"
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
lifecycle state advances to MixedVersions.`,
	Args: cobra.ExactArgs(1),
	RunE: runInstanceAdd,
}

var instanceRmCmd = &cobra.Command{
	Use:   "rm <instance-name>",
	Short: "Remove a Redis instance from its cluster",
	Long: `Removes a replica instance: issues REPLICAOF NO ONE, resets all sentinels
so they forget the removed instance, then stops and removes the container.

The current master (as reported by sentinel) cannot be removed; run
'russ sentinel failover' first to promote a different node.

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
	instanceAddCmd.Flags().String("max-memory", "64mb", "maxmemory limit for the new instance")

	instanceCmd.AddCommand(instanceAddCmd)
	instanceCmd.AddCommand(instanceRmCmd)
	instanceCmd.AddCommand(instanceLsCmd)
}

func runInstanceAdd(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	version, _ := cmd.Flags().GetInt("version")
	portRangeStr, _ := cmd.Flags().GetString("port-range")
	maxMemory, _ := cmd.Flags().GetString("max-memory")

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
		fmt.Fprintf(os.Stderr, "Sentinel errors:\n")
		for _, e := range sentinelErrs {
			fmt.Fprintf(os.Stderr, "  %s\n", e)
		}
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

	id, err := dm.StartRedis(ctx, docker.StartRedisOpts{
		ContainerName: containerName,
		Version:       version,
		HostPort:      port,
		Role:          docker.RoleReplica,
		ClusterName:   clusterName,
		MaxMemory:     maxMemory,
	})
	if err != nil {
		return err
	}

	if err := redisclient.WaitForReady(ctx, port); err != nil {
		return err
	}
	if err := redisclient.ConfigureReplica(ctx, port, masterContainerName, masterInfo.Port); err != nil {
		return err
	}
	if err := redisclient.WaitForReplication(ctx, port, masterInfo.Port); err != nil {
		return err
	}

	fmt.Printf("  ✓ replica %s (v%d) port=%d id=%.12s → master %s:%d\n",
		containerName, version, port, id, masterContainerName, masterInfo.Port)

	// Force every sentinel to re-probe so the new replica isn't sitting in the
	// gap between info-refresh cycles, then wait until at least one sentinel
	// confirms the discovery. This closes the race where a failover triggered
	// immediately after `instance add` would strand the new replica in a chain.
	for _, s := range sentinels {
		_ = redisclient.SentinelReset(ctx, s.Port, clusterName)
	}
	if err := redisclient.WaitForSentinelToSeeReplica(ctx, sentinels[0].Port, clusterName, port); err != nil {
		fmt.Fprintf(os.Stderr, "  warning: %v (failovers may not include this replica until sentinel re-polls)\n", err)
	} else {
		fmt.Printf("  ✓ sentinel %s sees %s as a replica\n", sentinels[0].Name, containerName)
	}

	// Advance the upgrade lifecycle state if applicable.
	currentState, _ := state.Load(clusterName)
	if version == 8 && currentState == state.StateAllV6 {
		machine, err := state.NewMachine(clusterName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not load state machine: %v\n", err)
		} else {
			if err := machine.Fire(ctx, state.TriggerAddV8Instance, nil); err != nil {
				fmt.Fprintf(os.Stderr, "warning: state transition failed: %v\n", err)
			} else {
				newState, _ := machine.Current(ctx)
				fmt.Printf("Upgrade state: %s → %s\n", currentState, newState)
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
				"%s is the current master for cluster %q; run 'russ sentinel failover %s' first",
				instanceName, clusterName, clusterName,
			)
		}
	}

	// Disconnect from replication.
	fmt.Printf("Issuing REPLICAOF NO ONE on %s (port %d)...\n", instanceName, ci.Port)
	if err := redisclient.BreakReplication(ctx, ci.Port); err != nil {
		fmt.Fprintf(os.Stderr, "  warning: %v (continuing with removal)\n", err)
	}

	// Reset all sentinels so they forget this instance.
	for _, s := range sentinels {
		if err := redisclient.SentinelReset(ctx, s.Port, clusterName); err != nil {
			fmt.Fprintf(os.Stderr, "  warning: SENTINEL RESET on %s: %v\n", s.Name, err)
		} else {
			fmt.Printf("  ✓ sentinel %s: RESET %s\n", s.Name, clusterName)
		}
	}

	if err := dm.StopAndRemove(ctx, ci.Name); err != nil {
		return err
	}
	fmt.Printf("  ✓ removed %s\n", instanceName)

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
				fmt.Printf("Upgrade state: %s → %s\n", currentState, newState)
			}
		}
	}

	printNextUpgradeStep(ctx, clusterName)
	return nil
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
