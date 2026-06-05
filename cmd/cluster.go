package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/bigcommerce/russ/internal/docker"
	redisclient "github.com/bigcommerce/russ/internal/redis"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Manage Redis Cluster installations",
}

var clusterCreateCmd = &cobra.Command{
	Use:   "create <cluster-name>",
	Short: "Create a Redis 8 Cluster",
	Long: `Creates a Redis 8 Cluster with --masters master nodes and
--replicas-per-master replicas per master, then runs CLUSTER MEET + slot
assignment + CLUSTER REPLICATE to wire everything together.

Redis Cluster requires at least 3 master nodes. With --replicas-per-master 1
(the default) you get 3 masters + 3 replicas = 6 nodes total.

Nodes announce their container name on the Docker network so other nodes and
the russ-client container can reach them via Docker DNS. Host-side connections
use 127.0.0.1:<port> via an address-rewriting Dialer.`,
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
	Args:  cobra.ExactArgs(1),
	RunE:  runClusterRm,
}

var clusterStatusCmd = &cobra.Command{
	Use:   "status <cluster-name>",
	Short: "Show cluster info and instance topology",
	Args:  cobra.ExactArgs(1),
	RunE:  runClusterStatus,
}

func init() {
	clusterCreateCmd.Flags().Int("masters", 3, "Number of master nodes (minimum 3)")
	clusterCreateCmd.Flags().Int("replicas-per-master", 1, "Replicas per master node (0 = no replicas)")
	clusterCreateCmd.Flags().String("port-range", "6380-6500", "Port range for instance allocation")
	clusterCreateCmd.Flags().String("max-memory", "256mb", "maxmemory limit applied to all instances")
	clusterCreateCmd.Flags().String("max-memory-policy", "volatile-lru", "Redis eviction policy when maxmemory is hit")
	clusterCreateCmd.Flags().Bool("enable-disk-persistence", true, "Enable AOF + RDB persistence")

	clusterCmd.AddCommand(clusterCreateCmd)
	clusterCmd.AddCommand(clusterLsCmd)
	clusterCmd.AddCommand(clusterRmCmd)
	clusterCmd.AddCommand(clusterStatusCmd)
}

func runClusterCreate(cmd *cobra.Command, args []string) error {
	clusterName := args[0]
	numMasters, _ := cmd.Flags().GetInt("masters")
	replicasPerMaster, _ := cmd.Flags().GetInt("replicas-per-master")
	portRangeStr, _ := cmd.Flags().GetString("port-range")
	maxMemory, _ := cmd.Flags().GetString("max-memory")
	maxMemoryPolicy, _ := cmd.Flags().GetString("max-memory-policy")
	persistence, _ := cmd.Flags().GetBool("enable-disk-persistence")

	if numMasters < 3 {
		return fmt.Errorf("--masters must be at least 3 (Redis Cluster minimum)")
	}
	if replicasPerMaster < 0 {
		return fmt.Errorf("--replicas-per-master must be >= 0")
	}

	totalNodes := numMasters * (replicasPerMaster + 1)

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

	existing, err := dm.ListClusterRedisInstances(ctx, clusterName)
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		return fmt.Errorf("cluster %q already exists (%d instances)", clusterName, len(existing))
	}

	if err := dm.EnsureNetwork(ctx); err != nil {
		return err
	}
	if err := dm.PullImage(ctx, docker.RedisVersion); err != nil {
		return err
	}

	log.Info().
		Str("cluster", clusterName).
		Int("masters", numMasters).
		Int("replicas_per_master", replicasPerMaster).
		Int("total_nodes", totalNodes).
		Msg("creating Redis Cluster")

	// Allocate all ports up front.
	ports := make([]int, totalNodes)
	for i := 0; i < totalNodes; i++ {
		p, err := dm.AllocatePort(ctx, pr)
		if err != nil {
			return fmt.Errorf("allocate port for node %d: %w", i+1, err)
		}
		ports[i] = p
		pr.Start = p + 1
	}

	// Start all nodes. The first numMasters are labelled master, the rest replica.
	// Redis Cluster assigns roles based on slot ownership + REPLICATE, not this label,
	// but the label lets russ track which containers to inspect.
	nodes := make([]docker.ContainerInfo, totalNodes)
	for i, port := range ports {
		role := docker.RoleMaster
		if i >= numMasters {
			role = docker.RoleReplica
		}
		name := docker.InstanceContainerName(clusterName, port)
		id, err := dm.StartRedis(ctx, docker.StartRedisOpts{
			ContainerName:   name,
			Version:         docker.RedisVersion,
			HostPort:        port,
			Role:            role,
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
			return dm.WrapWaitError(ctx, err, name)
		}
		nodes[i] = docker.ContainerInfo{Name: name, Port: port}
		log.Info().Str("name", name).Int("port", port).Str("id", id[:12]).Str("role", role).Msg("node ready")
	}

	// Wire the cluster: MEET + slot assignment + REPLICATE.
	if err := dm.InitRedisCluster(ctx, nodes, numMasters); err != nil {
		return fmt.Errorf("cluster init: %w", err)
	}

	log.Info().
		Str("cluster", clusterName).
		Int("masters", numMasters).
		Int("replicas", totalNodes-numMasters).
		Msg("cluster ready")
	return nil
}

func runClusterLs(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	dm, err := docker.New()
	if err != nil {
		return err
	}
	defer dm.Close()

	masters, err := dm.ListContainers(ctx, docker.LabelRole+"="+docker.RoleMaster)
	if err != nil {
		return err
	}
	if len(masters) == 0 {
		fmt.Println("No clusters found.")
		return nil
	}

	fmt.Printf("%-20s  %-9s  %-12s\n", "CLUSTER", "INSTANCES", "CLUSTER STATE")
	fmt.Println(strings.Repeat("-", 50))

	seen := map[string]bool{}
	for _, m := range masters {
		if seen[m.ClusterName] {
			continue
		}
		seen[m.ClusterName] = true

		instances, _ := dm.ListClusterRedisInstances(ctx, m.ClusterName)
		clusterState := "-"
		if len(instances) > 0 {
			if info, err := redisclient.ClusterInfo(ctx, instances[0].Port); err == nil {
				for _, line := range strings.Split(info, "\r\n") {
					if strings.HasPrefix(line, "cluster_state:") {
						clusterState = strings.TrimPrefix(line, "cluster_state:")
						break
					}
				}
			}
		}
		fmt.Printf("%-20s  %-9d  %s\n", m.ClusterName, len(instances), clusterState)
	}
	return nil
}

func runClusterRm(_ *cobra.Command, args []string) error {
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

	for _, ci := range instances {
		if err := dm.StopAndRemove(ctx, ci.Name); err != nil {
			log.Warn().Err(err).Str("name", ci.Name).Msg("remove container")
		} else {
			log.Info().Str("name", ci.Name).Msg("removed container")
		}
	}

	log.Info().Str("cluster", clusterName).Msg("cluster removed")
	return nil
}

func runClusterStatus(_ *cobra.Command, args []string) error {
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

	// Print CLUSTER INFO from the first reachable node.
	for _, ci := range instances {
		info, err := redisclient.ClusterInfo(ctx, ci.Port)
		if err != nil {
			continue
		}
		fmt.Printf("Cluster: %s\n\n", clusterName)
		// Print the most useful lines from CLUSTER INFO.
		for _, line := range strings.Split(strings.TrimSpace(info), "\r\n") {
			if line == "" {
				continue
			}
			switch {
			case strings.HasPrefix(line, "cluster_state:"),
				strings.HasPrefix(line, "cluster_slots_assigned:"),
				strings.HasPrefix(line, "cluster_slots_ok:"),
				strings.HasPrefix(line, "cluster_known_nodes:"),
				strings.HasPrefix(line, "cluster_size:"),
				strings.HasPrefix(line, "total_cluster_links_buffer_limit_exceeded:"):
				fmt.Printf("  %s\n", line)
			}
		}
		fmt.Println()
		break
	}

	printInstanceTable(ctx, instances, 0)
	return nil
}
