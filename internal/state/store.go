package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// State represents an upgrade lifecycle stage for a cluster.
type State string

const (
	// StateAllV6 is the initial state: all instances (sentinels + Redis) are on version 6.
	StateAllV6 State = "AllV6"
	// StateMixedVersions: at least one v8 instance has been added to the cluster.
	StateMixedVersions State = "MixedVersions"
	// StateFailoverComplete: sentinel has promoted a v8 node to master.
	StateFailoverComplete State = "FailoverComplete"
	// StateReplicationBroken: v6 replicas have been disconnected (REPLICAOF NO ONE).
	StateReplicationBroken State = "ReplicationBroken"
	// StateV6Destroyed: all v6 Redis containers removed from the cluster.
	StateV6Destroyed State = "V6Destroyed"
	// StateV8SentinelsAdded: new v8 sentinel instances have been bootstrapped.
	StateV8SentinelsAdded State = "V8SentinelsAdded"
	// StateDone: v6 sentinels removed; cluster fully on v8.
	StateDone State = "Done"
)

type stateFile struct {
	State State `json:"state"`
}

func stateDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("find home directory: %w", err)
	}
	dir := filepath.Join(home, ".russ", "state")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create state dir: %w", err)
	}
	return dir, nil
}

// Load reads the upgrade state for clusterName from disk.
// Returns StateAllV6 if no state has been persisted yet.
func Load(clusterName string) (State, error) {
	dir, err := stateDir()
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(dir, clusterName+".json"))
	if os.IsNotExist(err) {
		return StateAllV6, nil
	}
	if err != nil {
		return "", fmt.Errorf("read state for %s: %w", clusterName, err)
	}
	var sf stateFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return "", fmt.Errorf("parse state for %s: %w", clusterName, err)
	}
	return sf.State, nil
}

// Save persists the upgrade state for clusterName to disk.
func Save(clusterName string, s State) error {
	dir, err := stateDir()
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(stateFile{State: s}, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, clusterName+".json")
	return os.WriteFile(path, data, 0o644)
}

// ListClusters returns the names of all clusters that have persisted state.
func ListClusters() ([]string, error) {
	dir, err := stateDir()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read state dir: %w", err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, strings.TrimSuffix(e.Name(), ".json"))
		}
	}
	return names, nil
}

// Delete removes the persisted state for clusterName (called on cluster rm).
func Delete(clusterName string) error {
	dir, err := stateDir()
	if err != nil {
		return err
	}
	path := filepath.Join(dir, clusterName+".json")
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete state for %s: %w", clusterName, err)
	}
	return nil
}
