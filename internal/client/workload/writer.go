package workload

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// runWriter ticks at interval and performs INCR + SET against the sentinel-
// resolved master. Both commands exist in every Redis version we care about.
// The combined operation produces a monotonic counter plus a wall-clock
// timestamp the readers can use to compute replication lag.
func runWriter(ctx context.Context, client *redis.Client, interval time.Duration, m *Metrics) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			doWrite(ctx, client, m)
		}
	}
}

func doWrite(ctx context.Context, client *redis.Client, m *Metrics) {
	start := time.Now()

	// INCR returns the new value; we then write "<n>:<unix-nano>" so readers
	// can verify which "step" they're seeing and how long ago it was written.
	n, err := client.Incr(ctx, counterKey).Result()
	if err != nil {
		m.recordWrite(time.Since(start), err)
		return
	}
	err = client.Set(ctx, latestKey, formatValue(n, time.Now()), 0).Err()
	m.recordWrite(time.Since(start), err)
}
