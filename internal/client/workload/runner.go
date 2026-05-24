package workload

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// Runner coordinates the writer, master reader, and replica observer. All
// three share the same step interval; the observer also refreshes its replica
// list periodically on a longer cadence.
type Runner struct {
	SentinelAddrs   []string
	MasterName      string
	Interval        time.Duration // tick rate for all three roles
	RefreshInterval time.Duration // how often the observer re-asks sentinel for the replica list
	Metrics         *Metrics
}

// Run starts the three roles and blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	if r.Interval <= 0 {
		r.Interval = 200 * time.Millisecond
	}
	if r.RefreshInterval <= 0 {
		r.RefreshInterval = 10 * time.Second
	}

	// Writer + master reader share a failover client. It transparently follows
	// sentinel-driven master changes, so neither role has to re-resolve.
	failover := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    r.MasterName,
		SentinelAddrs: r.SentinelAddrs,
	})
	defer failover.Close()

	obs := newObserver(r.SentinelAddrs, r.MasterName, r.Metrics)

	log.Info().
		Str("cluster", r.MasterName).
		Dur("interval", r.Interval).
		Dur("refresh", r.RefreshInterval).
		Msg("client running: writer + master reader + replica observer")

	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); runWriter(ctx, failover, r.Interval, r.Metrics) }()
	go func() { defer wg.Done(); runMasterReader(ctx, failover, r.Interval, r.Metrics) }()
	go func() { defer wg.Done(); obs.run(ctx, r.Interval, r.RefreshInterval) }()
	wg.Wait()
	return nil
}
