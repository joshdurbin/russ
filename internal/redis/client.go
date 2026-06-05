package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

func addr(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func newClient(port int) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:        addr(port),
		DialTimeout: 3 * time.Second,
	})
}

// WaitForReady polls the instance at the given host port until it responds to PING,
// or until ctx is cancelled / 30 s elapses.
func WaitForReady(ctx context.Context, port int) error {
	log.Debug().Int("port", port).Msg("waiting for redis to PING")
	rdb := newClient(port)
	defer rdb.Close()

	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := rdb.Ping(ctx).Err(); err == nil {
			log.Debug().Int("port", port).Msg("redis PING ok")
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("redis on port %d did not become ready within 30s", port)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// BreakReplication issues REPLICAOF NO ONE, making the instance a standalone master.
func BreakReplication(ctx context.Context, port int) error {
	rdb := newClient(port)
	defer rdb.Close()
	if err := rdb.Do(ctx, "REPLICAOF", "NO", "ONE").Err(); err != nil {
		return fmt.Errorf("REPLICAOF NO ONE on port %d: %w", port, err)
	}
	return nil
}

// ReplicationState captures a Redis node's own view of its replication role.
type ReplicationState struct {
	Role             string
	MasterHost       string
	MasterPort       int
	MasterLinkStatus string
}

// GetReplicationState reads INFO replication and returns the parsed state.
func GetReplicationState(ctx context.Context, port int) (ReplicationState, error) {
	rdb := newClient(port)
	defer rdb.Close()
	info, err := rdb.Info(ctx, "replication").Result()
	if err != nil {
		return ReplicationState{}, fmt.Errorf("INFO replication on port %d: %w", port, err)
	}
	var s ReplicationState
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "role:"):
			s.Role = strings.TrimPrefix(line, "role:")
		case strings.HasPrefix(line, "master_host:"):
			s.MasterHost = strings.TrimPrefix(line, "master_host:")
		case strings.HasPrefix(line, "master_port:"):
			s.MasterPort, _ = strconv.Atoi(strings.TrimPrefix(line, "master_port:"))
		case strings.HasPrefix(line, "master_link_status:"):
			s.MasterLinkStatus = strings.TrimPrefix(line, "master_link_status:")
		}
	}
	return s, nil
}

// MemoryUsage captures the Redis-reported and OS-reported memory state.
type MemoryUsage struct {
	UsedMemory            int64
	UsedMemoryRSS         int64
	UsedMemoryPeak        int64
	MemFragmentationRatio float64
	MaxMemory             int64
}

// GetMemoryUsage reads INFO memory and parses out the load-bearing fields.
func GetMemoryUsage(ctx context.Context, port int) (MemoryUsage, error) {
	rdb := newClient(port)
	defer rdb.Close()
	info, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		return MemoryUsage{}, fmt.Errorf("INFO memory on port %d: %w", port, err)
	}
	var m MemoryUsage
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "used_memory:"):
			m.UsedMemory, _ = strconv.ParseInt(strings.TrimPrefix(line, "used_memory:"), 10, 64)
		case strings.HasPrefix(line, "used_memory_rss:"):
			m.UsedMemoryRSS, _ = strconv.ParseInt(strings.TrimPrefix(line, "used_memory_rss:"), 10, 64)
		case strings.HasPrefix(line, "used_memory_peak:"):
			m.UsedMemoryPeak, _ = strconv.ParseInt(strings.TrimPrefix(line, "used_memory_peak:"), 10, 64)
		case strings.HasPrefix(line, "mem_fragmentation_ratio:"):
			m.MemFragmentationRatio, _ = strconv.ParseFloat(strings.TrimPrefix(line, "mem_fragmentation_ratio:"), 64)
		case strings.HasPrefix(line, "maxmemory:"):
			m.MaxMemory, _ = strconv.ParseInt(strings.TrimPrefix(line, "maxmemory:"), 10, 64)
		}
	}
	return m, nil
}

// --- Redis Cluster commands ---

// ClusterNode represents a node as reported by CLUSTER NODES.
type ClusterNode struct {
	ID       string
	Host     string
	Port     int
	IsMaster bool
	MasterID string // non-empty for replicas
}

// ClusterMeet tells the node at port to connect to targetHost:targetPort.
func ClusterMeet(ctx context.Context, port int, targetHost string, targetPort int) error {
	rdb := newClient(port)
	defer rdb.Close()
	if err := rdb.Do(ctx, "CLUSTER", "MEET", targetHost, strconv.Itoa(targetPort)).Err(); err != nil {
		return fmt.Errorf("CLUSTER MEET on port %d → %s:%d: %w", port, targetHost, targetPort, err)
	}
	return nil
}

// ClusterAddSlotsRange assigns a contiguous range of hash slots to the node at port.
// Requires Redis 7+; Redis 8 supports this natively.
func ClusterAddSlotsRange(ctx context.Context, port int, startSlot, endSlot int) error {
	rdb := newClient(port)
	defer rdb.Close()
	if err := rdb.Do(ctx, "CLUSTER", "ADDSLOTSRANGE", startSlot, endSlot).Err(); err != nil {
		return fmt.Errorf("CLUSTER ADDSLOTSRANGE %d-%d on port %d: %w", startSlot, endSlot, port, err)
	}
	return nil
}

// ClusterMyID returns the node's own cluster node ID.
func ClusterMyID(ctx context.Context, port int) (string, error) {
	rdb := newClient(port)
	defer rdb.Close()
	id, err := rdb.Do(ctx, "CLUSTER", "MYID").Text()
	if err != nil {
		return "", fmt.Errorf("CLUSTER MYID on port %d: %w", port, err)
	}
	return strings.TrimSpace(id), nil
}

// ClusterReplicate makes the node at port a replica of masterNodeID.
func ClusterReplicate(ctx context.Context, port int, masterNodeID string) error {
	rdb := newClient(port)
	defer rdb.Close()
	if err := rdb.Do(ctx, "CLUSTER", "REPLICATE", masterNodeID).Err(); err != nil {
		return fmt.Errorf("CLUSTER REPLICATE on port %d: %w", port, err)
	}
	return nil
}

// ClusterForget removes nodeID from the cluster's view as seen from port.
func ClusterForget(ctx context.Context, port int, nodeID string) error {
	rdb := newClient(port)
	defer rdb.Close()
	if err := rdb.Do(ctx, "CLUSTER", "FORGET", nodeID).Err(); err != nil {
		return fmt.Errorf("CLUSTER FORGET %s on port %d: %w", nodeID, port, err)
	}
	return nil
}

// ClusterReset performs a soft reset on the node at port, clearing its cluster membership.
func ClusterReset(ctx context.Context, port int) error {
	rdb := newClient(port)
	defer rdb.Close()
	return rdb.Do(ctx, "CLUSTER", "RESET", "SOFT").Err()
}

// ClusterInfo returns the raw CLUSTER INFO text from the node at port.
func ClusterInfo(ctx context.Context, port int) (string, error) {
	rdb := newClient(port)
	defer rdb.Close()
	return rdb.Do(ctx, "CLUSTER", "INFO").Text()
}

// ClusterNodes parses the CLUSTER NODES output from the node at port.
func ClusterNodes(ctx context.Context, port int) ([]ClusterNode, error) {
	rdb := newClient(port)
	defer rdb.Close()
	result, err := rdb.Do(ctx, "CLUSTER", "NODES").Text()
	if err != nil {
		return nil, fmt.Errorf("CLUSTER NODES on port %d: %w", port, err)
	}
	var nodes []ClusterNode
	for _, line := range strings.Split(strings.TrimSpace(result), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 8 {
			continue
		}
		// addr field is "hostname:port@busport"
		addrField := strings.Split(parts[1], "@")[0]
		colonIdx := strings.LastIndex(addrField, ":")
		nodeHost := addrField[:colonIdx]
		nodePort, _ := strconv.Atoi(addrField[colonIdx+1:])

		flags := parts[2]
		masterField := parts[3]
		isMaster := strings.Contains(flags, "master") && !strings.Contains(flags, "slave")
		masterID := ""
		if !isMaster && masterField != "-" {
			masterID = masterField
		}
		nodes = append(nodes, ClusterNode{
			ID:       parts[0],
			Host:     nodeHost,
			Port:     nodePort,
			IsMaster: isMaster,
			MasterID: masterID,
		})
	}
	return nodes, nil
}

// WaitForClusterNodes polls until the node at port sees at least expectedCount nodes in CLUSTER NODES.
func WaitForClusterNodes(ctx context.Context, port int, expectedCount int) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		nodes, err := ClusterNodes(ctx, port)
		if err == nil && len(nodes) >= expectedCount {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout: cluster at port %d has not reached %d nodes within 30s", port, expectedCount)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// WaitForClusterNodeID polls CLUSTER NODES on port until nodeID appears in the local view.
// Called before CLUSTER REPLICATE to ensure the replica has received the master's ID via gossip.
func WaitForClusterNodeID(ctx context.Context, port int, nodeID string) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		nodes, err := ClusterNodes(ctx, port)
		if err == nil {
			for _, n := range nodes {
				if n.ID == nodeID {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("node %s not visible in CLUSTER NODES on port %d after 15s", nodeID[:8], port)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// WaitForClusterOK polls CLUSTER INFO until cluster_state:ok or the deadline elapses.
func WaitForClusterOK(ctx context.Context, port int) error {
	log.Debug().Int("port", port).Msg("waiting for cluster_state:ok")
	deadline := time.Now().Add(60 * time.Second)
	for {
		info, err := ClusterInfo(ctx, port)
		if err == nil && strings.Contains(info, "cluster_state:ok") {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster at port %d did not reach state:ok within 60s", port)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}
