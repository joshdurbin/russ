package client

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// MasterInfo describes one Redis master discovered via Sentinel.
type MasterInfo struct {
	Name string
	Host string
	Port int
}

// DiscoverMasters connects to the first reachable sentinel in sentinelAddrs and
// returns every master it monitors. Sentinels are tried in order; the first one
// that responds wins.
func DiscoverMasters(ctx context.Context, sentinelAddrs []string) ([]MasterInfo, error) {
	if len(sentinelAddrs) == 0 {
		return nil, fmt.Errorf("no sentinel addresses provided")
	}

	var lastErr error
	for _, addr := range sentinelAddrs {
		// Force RESP2: in RESP3 sentinel returns maps, but Masters().Result()
		// is typed as []interface{} of flat alternating key/value arrays.
		sc := redis.NewSentinelClient(&redis.Options{Addr: addr, Protocol: 2})
		result, err := sc.Masters(ctx).Result()
		_ = sc.Close()
		if err != nil {
			lastErr = fmt.Errorf("sentinel %s: %w", addr, err)
			continue
		}
		masters := make([]MasterInfo, 0, len(result))
		for _, m := range result {
			fields, ok := m.([]interface{})
			if !ok {
				continue
			}
			info := map[string]string{}
			for i := 0; i+1 < len(fields); i += 2 {
				k, _ := fields[i].(string)
				v, _ := fields[i+1].(string)
				info[k] = v
			}
			name := info["name"]
			if name == "" {
				continue
			}
			port, _ := strconv.Atoi(info["port"])
			masters = append(masters, MasterInfo{Name: name, Host: info["ip"], Port: port})
		}
		return masters, nil
	}
	return nil, fmt.Errorf("could not contact any of %d sentinel(s): %w", len(sentinelAddrs), lastErr)
}

// DiscoverReplicas asks the first reachable sentinel for the replicas of
// masterName and returns them as a slice of MasterInfo (Host:Port pairs).
func DiscoverReplicas(ctx context.Context, sentinelAddrs []string, masterName string) ([]MasterInfo, error) {
	if len(sentinelAddrs) == 0 {
		return nil, fmt.Errorf("no sentinel addresses provided")
	}
	var lastErr error
	for _, addr := range sentinelAddrs {
		// Use a regular client (not SentinelClient) with Protocol: 2 so the
		// SENTINEL REPLICAS response comes back as flat key/value arrays
		// rather than RESP3 maps.
		c := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2, DialTimeout: 2 * time.Second})
		result, err := c.Do(ctx, "SENTINEL", "REPLICAS", masterName).Slice()
		_ = c.Close()
		if err != nil {
			lastErr = fmt.Errorf("sentinel %s: %w", addr, err)
			continue
		}
		out := make([]MasterInfo, 0, len(result))
		for _, item := range result {
			fields, ok := item.([]interface{})
			if !ok {
				continue
			}
			info := map[string]string{}
			for i := 0; i+1 < len(fields); i += 2 {
				k, _ := fields[i].(string)
				v, _ := fields[i+1].(string)
				info[k] = v
			}
			port, _ := strconv.Atoi(info["port"])
			if info["ip"] == "" || port == 0 {
				continue
			}
			out = append(out, MasterInfo{
				Name: fmt.Sprintf("%s:%d", info["ip"], port),
				Host: info["ip"],
				Port: port,
			})
		}
		return out, nil
	}
	return nil, fmt.Errorf("could not contact any of %d sentinel(s): %w", len(sentinelAddrs), lastErr)
}

// PickMaster returns the master with the given name. If name is empty and
// exactly one master was discovered, that master is returned. Otherwise an
// error lists the available master names.
func PickMaster(masters []MasterInfo, name string) (MasterInfo, error) {
	if len(masters) == 0 {
		return MasterInfo{}, fmt.Errorf("no masters discovered")
	}
	if name == "" {
		if len(masters) == 1 {
			return masters[0], nil
		}
		available := make([]string, len(masters))
		for i, m := range masters {
			available[i] = m.Name
		}
		return MasterInfo{}, fmt.Errorf("multiple masters discovered, pass --cluster to choose one of: %v", available)
	}
	for _, m := range masters {
		if m.Name == name {
			return m, nil
		}
	}
	available := make([]string, len(masters))
	for i, m := range masters {
		available[i] = m.Name
	}
	return MasterInfo{}, fmt.Errorf("master %q not found; available: %v", name, available)
}
