package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
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
	rdb := newClient(port)
	defer rdb.Close()

	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := rdb.Ping(ctx).Err(); err == nil {
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

// SetMaxMemory configures maxmemory and allkeys-lru eviction on a Redis instance.
func SetMaxMemory(ctx context.Context, port int, size string) error {
	rdb := newClient(port)
	defer rdb.Close()
	if err := rdb.ConfigSet(ctx, "maxmemory", size).Err(); err != nil {
		return fmt.Errorf("CONFIG SET maxmemory on port %d: %w", port, err)
	}
	return rdb.ConfigSet(ctx, "maxmemory-policy", "allkeys-lru").Err()
}

// ConfigureReplica issues REPLICAOF masterContainerName masterPort on the replica.
// masterContainerName is the Docker container name, resolvable within the russ network.
func ConfigureReplica(ctx context.Context, replicaPort int, masterContainerName string, masterPort int) error {
	rdb := newClient(replicaPort)
	defer rdb.Close()
	if err := rdb.Do(ctx, "REPLICAOF", masterContainerName, strconv.Itoa(masterPort)).Err(); err != nil {
		return fmt.Errorf("REPLICAOF on replica port %d: %w", replicaPort, err)
	}
	return nil
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

// SetReplicaPriority sets the replica-priority config on a Redis instance.
// Lower priority = preferred for promotion. 0 = never promote.
func SetReplicaPriority(ctx context.Context, port int, priority int) error {
	rdb := newClient(port)
	defer rdb.Close()
	if err := rdb.ConfigSet(ctx, "replica-priority", strconv.Itoa(priority)).Err(); err != nil {
		return fmt.Errorf("CONFIG SET replica-priority on port %d: %w", port, err)
	}
	return nil
}

// -- Sentinel management commands --

func newSentinelClient(sentinelPort int) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:        addr(sentinelPort),
		DialTimeout: 3 * time.Second,
		Protocol:    2, // RESP2: sentinel commands return flat arrays, not RESP3 maps
	})
}

// SentinelMonitor tells a sentinel to begin monitoring a master.
// quorum is the number of sentinels that must agree for a failover.
func SentinelMonitor(ctx context.Context, sentinelPort int, masterName, masterHost string, masterPort, quorum int) error {
	rdb := newSentinelClient(sentinelPort)
	defer rdb.Close()
	err := rdb.Do(ctx, "SENTINEL", "MONITOR",
		masterName, masterHost, strconv.Itoa(masterPort), strconv.Itoa(quorum),
	).Err()
	if err != nil {
		return fmt.Errorf("SENTINEL MONITOR on sentinel %d: %w", sentinelPort, err)
	}
	// Configure reasonable defaults.
	_ = rdb.Do(ctx, "SENTINEL", "SET", masterName, "down-after-milliseconds", "5000").Err()
	_ = rdb.Do(ctx, "SENTINEL", "SET", masterName, "failover-timeout", "30000").Err()
	_ = rdb.Do(ctx, "SENTINEL", "SET", masterName, "parallel-syncs", "1").Err()
	return nil
}

// SentinelSetQuorum updates the quorum required for a failover on an already-monitored master.
func SentinelSetQuorum(ctx context.Context, sentinelPort int, masterName string, quorum int) error {
	rdb := newSentinelClient(sentinelPort)
	defer rdb.Close()
	if err := rdb.Do(ctx, "SENTINEL", "SET", masterName, "quorum", strconv.Itoa(quorum)).Err(); err != nil {
		return fmt.Errorf("SENTINEL SET quorum on sentinel %d: %w", sentinelPort, err)
	}
	return nil
}

// SentinelRemove tells a sentinel to stop monitoring a master entirely.
func SentinelRemove(ctx context.Context, sentinelPort int, masterName string) error {
	rdb := newSentinelClient(sentinelPort)
	defer rdb.Close()
	if err := rdb.Do(ctx, "SENTINEL", "REMOVE", masterName).Err(); err != nil {
		return fmt.Errorf("SENTINEL REMOVE on sentinel %d: %w", sentinelPort, err)
	}
	return nil
}

// SentinelReset tells a sentinel to reset its topology state for a master,
// causing it to re-probe and forget any disappeared replicas.
func SentinelReset(ctx context.Context, sentinelPort int, masterName string) error {
	rdb := newSentinelClient(sentinelPort)
	defer rdb.Close()
	if err := rdb.Do(ctx, "SENTINEL", "RESET", masterName).Err(); err != nil {
		return fmt.Errorf("SENTINEL RESET on sentinel %d: %w", sentinelPort, err)
	}
	return nil
}

// SentinelFailover triggers a manual failover for masterName on the given sentinel.
func SentinelFailover(ctx context.Context, sentinelPort int, masterName string) error {
	rdb := newSentinelClient(sentinelPort)
	defer rdb.Close()
	if err := rdb.Do(ctx, "SENTINEL", "FAILOVER", masterName).Err(); err != nil {
		return fmt.Errorf("SENTINEL FAILOVER on sentinel %d: %w", sentinelPort, err)
	}
	return nil
}

// MasterInfo holds the current master's address as reported by a sentinel.
type MasterInfo struct {
	Host string
	Port int
}

// SentinelGetMaster queries a sentinel for the current master of masterName.
func SentinelGetMaster(ctx context.Context, sentinelPort int, masterName string) (MasterInfo, error) {
	rdb := newSentinelClient(sentinelPort)
	defer rdb.Close()

	result, err := rdb.Do(ctx, "SENTINEL", "MASTER", masterName).Slice()
	if err != nil {
		return MasterInfo{}, fmt.Errorf("SENTINEL MASTER on sentinel %d: %w", sentinelPort, err)
	}

	kv := flatSliceToMap(result)
	host := kv["ip"]
	portStr := kv["port"]
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return MasterInfo{}, fmt.Errorf("invalid port in SENTINEL MASTER response: %q", portStr)
	}
	return MasterInfo{Host: host, Port: port}, nil
}

// SentinelListMasters returns all master names currently monitored by a sentinel.
func SentinelListMasters(ctx context.Context, sentinelPort int) ([]string, error) {
	rdb := newSentinelClient(sentinelPort)
	defer rdb.Close()

	result, err := rdb.Do(ctx, "SENTINEL", "MASTERS").Slice()
	if err != nil {
		return nil, fmt.Errorf("SENTINEL MASTERS on sentinel %d: %w", sentinelPort, err)
	}

	var names []string
	for _, item := range result {
		switch v := item.(type) {
		case []interface{}:
			kv := flatSliceToMap(v)
			if name := kv["name"]; name != "" {
				names = append(names, name)
			}
		}
	}
	return names, nil
}

// SentinelListReplicas returns the replica addresses for masterName as reported by a sentinel.
func SentinelListReplicas(ctx context.Context, sentinelPort int, masterName string) ([]MasterInfo, error) {
	rdb := newSentinelClient(sentinelPort)
	defer rdb.Close()

	result, err := rdb.Do(ctx, "SENTINEL", "REPLICAS", masterName).Slice()
	if err != nil {
		return nil, fmt.Errorf("SENTINEL REPLICAS on sentinel %d: %w", sentinelPort, err)
	}

	var replicas []MasterInfo
	for _, item := range result {
		switch v := item.(type) {
		case []interface{}:
			kv := flatSliceToMap(v)
			port, _ := strconv.Atoi(kv["port"])
			replicas = append(replicas, MasterInfo{Host: kv["ip"], Port: port})
		}
	}
	return replicas, nil
}

// ReplicaState captures sentinel's most recently observed view of a replica,
// including the replica-priority it saw on the last INFO refresh.
type ReplicaState struct {
	Host     string
	Port     int
	Priority int
}

// SentinelReplicaStates returns sentinel's view of each replica including the
// priority it last observed via INFO. Important: this is *sentinel's view*,
// which lags the replica's actual config by up to one info-refresh interval
// (~10s by default) after a CONFIG SET replica-priority.
func SentinelReplicaStates(ctx context.Context, sentinelPort int, masterName string) ([]ReplicaState, error) {
	rdb := newSentinelClient(sentinelPort)
	defer rdb.Close()

	result, err := rdb.Do(ctx, "SENTINEL", "REPLICAS", masterName).Slice()
	if err != nil {
		return nil, fmt.Errorf("SENTINEL REPLICAS on sentinel %d: %w", sentinelPort, err)
	}

	var states []ReplicaState
	for _, item := range result {
		v, ok := item.([]interface{})
		if !ok {
			continue
		}
		kv := flatSliceToMap(v)
		port, _ := strconv.Atoi(kv["port"])
		// Sentinel reports "slave-priority" regardless of Redis version (the
		// field name is kept for backwards compatibility).
		prio, _ := strconv.Atoi(kv["slave-priority"])
		states = append(states, ReplicaState{
			Host:     kv["ip"],
			Port:     port,
			Priority: prio,
		})
	}
	return states, nil
}

// WaitForReplicaPriorities polls sentinel until every port in expected shows
// the priority value the caller specifies in the map. Used after CONFIG SET
// replica-priority on the replica itself to ensure sentinel has refreshed its
// cached INFO and will honor the new priority during a SENTINEL FAILOVER.
//
// expected maps replica port → expected priority. Replicas not in the map are
// ignored. Returns when every expected port matches, or when deadline elapses.
func WaitForReplicaPriorities(ctx context.Context, sentinelPort int, masterName string, expected map[int]int, timeout time.Duration) error {
	if len(expected) == 0 {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for {
		states, err := SentinelReplicaStates(ctx, sentinelPort, masterName)
		if err == nil {
			seen := make(map[int]int, len(states))
			for _, s := range states {
				seen[s.Port] = s.Priority
			}
			satisfied := true
			for port, want := range expected {
				if got, ok := seen[port]; !ok || got != want {
					satisfied = false
					break
				}
			}
			if satisfied {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sentinel did not observe expected replica priorities for master %q within %s", masterName, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

// WaitForFailover polls a sentinel until the current master port for masterName
// matches one of acceptablePorts, indicating the failover has completed.
// Returns the port that was promoted.
func WaitForFailover(ctx context.Context, sentinelPort int, masterName string, acceptablePorts ...int) (int, error) {
	if len(acceptablePorts) == 0 {
		return 0, fmt.Errorf("no acceptable target ports given")
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		info, err := SentinelGetMaster(ctx, sentinelPort, masterName)
		if err == nil {
			for _, p := range acceptablePorts {
				if info.Port == p {
					return p, nil
				}
			}
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("failover to one of %v did not complete within 60s", acceptablePorts)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// WaitForReplication polls until the replica at replicaPort reports it is connected
// to the master at masterPort.
func WaitForReplication(ctx context.Context, replicaPort int, masterPort int) error {
	rdb := newClient(replicaPort)
	defer rdb.Close()

	deadline := time.Now().Add(30 * time.Second)
	for {
		info, err := rdb.Info(ctx, "replication").Result()
		if err == nil {
			if contains(info, fmt.Sprintf("master_port:%d", masterPort)) &&
				contains(info, "master_link_status:up") {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("replica on port %d did not sync to master %d within 30s", replicaPort, masterPort)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ReplicationState captures a Redis node's own view of its replication role,
// the master it's connected to, and the health of that link.
type ReplicationState struct {
	Role             string // "master" or "slave"
	MasterHost       string // empty when Role == "master"
	MasterPort       int    // 0 when Role == "master"
	MasterLinkStatus string // "up" or "down"; empty when Role == "master"
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

// ReplicationTarget returns the master_port a replica is connected to, plus its
// master_host string. Returns ("", 0, nil) if the node reports itself as master.
func ReplicationTarget(ctx context.Context, port int) (string, int, error) {
	s, err := GetReplicationState(ctx, port)
	if err != nil {
		return "", 0, err
	}
	if s.Role == "master" {
		return "", 0, nil
	}
	return s.MasterHost, s.MasterPort, nil
}

// WaitForSentinelToSeeReplica polls a sentinel until SENTINEL REPLICAS for the
// named master includes replicaPort, or until the 30s deadline elapses.
func WaitForSentinelToSeeReplica(ctx context.Context, sentinelPort int, masterName string, replicaPort int) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		replicas, err := SentinelListReplicas(ctx, sentinelPort, masterName)
		if err == nil {
			for _, r := range replicas {
				if r.Port == replicaPort {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("sentinel did not discover replica on port %d for master %q within 30s", replicaPort, masterName)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// -- helpers --

func flatSliceToMap(slice []interface{}) map[string]string {
	m := make(map[string]string, len(slice)/2)
	for i := 0; i+1 < len(slice); i += 2 {
		k, _ := slice[i].(string)
		v, _ := slice[i+1].(string)
		if k != "" {
			m[k] = v
		}
	}
	return m
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
