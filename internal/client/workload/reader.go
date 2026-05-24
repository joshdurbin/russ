package workload

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// runMasterReader ticks at interval, GETs latestKey from the master via the
// failover client, and emits both the counter value and its age. The age is
// the dominant health signal: a master that's accepting writes but the
// writer stalled would show a growing master_value_age_seconds while writes
// stay flat.
func runMasterReader(ctx context.Context, client *redis.Client, interval time.Duration, m *Metrics) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			doMasterRead(ctx, client, m)
		}
	}
}

func doMasterRead(ctx context.Context, client *redis.Client, m *Metrics) {
	start := time.Now()
	val, err := client.Get(ctx, latestKey).Result()
	dur := time.Since(start)
	m.recordMasterRead(dur, err)
	if err != nil {
		return
	}
	n, writtenAt, ok := parseValue(val)
	if !ok {
		return
	}
	m.recordMasterValue(n, writtenAt)
}
