package cmd

import (
	"context"
	"fmt"

	"github.com/bigcommerce/russ/internal/docker"
	redisclient "github.com/bigcommerce/russ/internal/redis"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var instanceCmd = &cobra.Command{
	Use:   "instance",
	Short: "Manage individual Redis instances within a cluster",
}

var instanceAddCmd = &cobra.Command{
	Use:   "add <cluster-name>",
	Short: "Add a Redis 8 replica to an existing cluster",
	Long: `Creates a new Redis 8 replica node, joins it to the cluster gossip ring
via CLUSTER MEET, and assigns it to the master with the fewest current replicas
via CLUSTER REPLICATE.`,
	Args: cobra.ExactArgs(1),
	RunE: runInstanceAdd,
}

var instanceRmCmd = &cobra.Command{
	Use:   "rm <instance-name>",
	Short: "Remove a replica instance from its cluster",
	Long: `Issues CLUSTER FORGET on all other cluster nodes (removing the node from
the gossip ring), resets the departing node, then stops and removes the container.

Only replicas can be removed. Master nodes own hash slots and cannot be removed
without a prior failover and slot migration.`,
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
	instanceAddCmd.Flags().String("port-range", "6380-6500", "Port range for instance allocation")
	instanceAddCmd.Flags().String("max-memory", "256mb", "maxmemory limit for the new instance")
	instanceAddCmd.Flags().String("max-memory-policy", "volatile-lru", "Redis eviction policy when maxmemory is hit")
	instanceAddCmd.Flags().Bool("enable-disk-persistence", true, "Enable AOF + RDB persistence")

	instanceCmd.AddCommand(instanceAddCmd)
	instanceCmd.AddCommand(instanceRmCmd)
	instanceCmd.AddCommand(instanceLsCmd)
}

func runInstanceAdd(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	portRangeStr, _ := cmd.Flags().GetString("port-range")
	maxMemory, _ := cmd.Flags().GetString("max-memory")
	maxMemoryPolicy, _ := cmd.Flags().GetString("max-memory-policy")
	persistence, _ := cmd.Flags().GetBool("enable-disk-persistence")

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

	instances, err := dm.ListClusterRedisInstances(ctx, clusterName)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		return fmt.Errorf("cluster %q not found; create it with 'russ cluster create %s'", clusterName, clusterName)
	}

	// Get the current cluster topology from any reachable node.
	var clusterNodes []redisclient.ClusterNode
	for _, ci := range instances {
		nodes, err := redisclient.ClusterNodes(ctx, ci.Port)
		if err == nil {
			clusterNodes = nodes
			break
		}
	}
	if len(clusterNodes) == 0 {
		return fmt.Errorf("could not query CLUSTER NODES on any instance in %q", clusterName)
	}

	// Pick the master with the fewest replicas.
	replicaCount := map[string]int{}
	for _, n := range clusterNodes {
		if !n.IsMaster {
			replicaCount[n.MasterID]++
		}
	}
	targetMasterID := ""
	minReplicas := -1
	for _, n := range clusterNodes {
		if !n.IsMaster {
			continue
		}
		count := replicaCount[n.ID]
		if minReplicas < 0 || count < minReplicas {
			minReplicas = count
			targetMasterID = n.ID
		}
	}
	if targetMasterID == "" {
		return fmt.Errorf("no master found in cluster %q", clusterName)
	}
	log.Info().Str("master_id", targetMasterID[:8]).Int("current_replicas", minReplicas).Msg("selected target master")

	if err := dm.PullImage(ctx, docker.RedisVersion); err != nil {
		return err
	}

	port, err := dm.AllocatePort(ctx, pr)
	if err != nil {
		return err
	}
	containerName := docker.InstanceContainerName(clusterName, port)

	id, err := dm.StartRedis(ctx, docker.StartRedisOpts{
		ContainerName:   containerName,
		Version:         docker.RedisVersion,
		HostPort:        port,
		Role:            docker.RoleReplica,
		ClusterName:     clusterName,
		MaxMemory:       maxMemory,
		MaxMemoryPolicy: maxMemoryPolicy,
		Persistence:     persistence,
		ClusterMode:     true,
	})
	if err != nil {
		return err
	}
	if err := redisclient.WaitForReady(ctx, port); err != nil {
		return dm.WrapWaitError(ctx, err, containerName)
	}

	// Join the new node to the cluster gossip ring using an existing node's Docker IP.
	existingDockerIP, err := dm.GetContainerNetworkIP(ctx, instances[0].Name)
	if err != nil {
		return fmt.Errorf("get docker IP for %s: %w", instances[0].Name, err)
	}
	if err := redisclient.ClusterMeet(ctx, port, existingDockerIP, instances[0].Port); err != nil {
		return err
	}

	// Wait for the new node to be visible in the cluster.
	if err := redisclient.WaitForClusterNodes(ctx, instances[0].Port, len(clusterNodes)+1); err != nil {
		return fmt.Errorf("new node did not join cluster gossip ring: %w", err)
	}

	// Get the new node's cluster ID (needed for REPLICATE).
	newNodeID, err := redisclient.ClusterMyID(ctx, port)
	if err != nil {
		return err
	}

	if err := redisclient.ClusterReplicate(ctx, port, targetMasterID); err != nil {
		return err
	}

	log.Info().
		Str("name", containerName).
		Int("port", port).
		Str("id", id[:12]).
		Str("node_id", newNodeID[:8]).
		Str("replicating_master", targetMasterID[:8]).
		Msg("replica added to cluster")
	return nil
}

func runInstanceRm(_ *cobra.Command, args []string) error {
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
	if ci.ClusterName == "" {
		return fmt.Errorf("container %s has no cluster label", instanceName)
	}

	clusterName := ci.ClusterName

	// Get the node's cluster ID before we remove anything.
	nodeID, err := redisclient.ClusterMyID(ctx, ci.Port)
	if err != nil {
		return fmt.Errorf("get cluster node ID for %s: %w", instanceName, err)
	}

	// Verify this is a replica (not a master that owns slots).
	nodes, err := redisclient.ClusterNodes(ctx, ci.Port)
	if err != nil {
		return fmt.Errorf("CLUSTER NODES on %s: %w", instanceName, err)
	}
	for _, n := range nodes {
		if n.ID == nodeID && n.IsMaster {
			return fmt.Errorf(
				"%s is a cluster master — migrate its slots to another master before removing",
				instanceName,
			)
		}
	}

	// Forget this node on every other cluster member.
	instances, _ := dm.ListClusterRedisInstances(ctx, clusterName)
	for _, other := range instances {
		if other.Port == ci.Port {
			continue
		}
		if err := redisclient.ClusterForget(ctx, other.Port, nodeID); err != nil {
			log.Warn().Err(err).Str("from", other.Name).Str("node_id", nodeID[:8]).Msg("CLUSTER FORGET")
		}
	}

	// Soft-reset the departing node so it forgets its cluster membership.
	_ = redisclient.ClusterReset(ctx, ci.Port)

	if err := dm.StopAndRemove(ctx, ci.Name); err != nil {
		return err
	}
	log.Info().Str("name", instanceName).Msg("removed instance")
	return nil
}

func runInstanceLs(_ *cobra.Command, args []string) error {
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

	printInstanceTable(ctx, instances, 0)
	return nil
}
