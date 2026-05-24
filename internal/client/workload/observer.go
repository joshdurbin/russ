package workload

import (
	"context"
	"sync"
	"time"

	"github.com/bigcommerce/russ/internal/client"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// observer holds a per-replica connection pool and polls each replica's
// latestKey on every tick. Replicas come and go (sentinel-driven topology),
// so the set is refreshed on a separate timer.
type observer struct {
	sentinelAddrs []string
	masterName    string
	metrics       *Metrics

	mu      sync.Mutex
	clients map[string]*redis.Client // addr → client
}

func newObserver(sentinelAddrs []string, masterName string, metrics *Metrics) *observer {
	return &observer{
		sentinelAddrs: sentinelAddrs,
		masterName:    masterName,
		metrics:       metrics,
		clients:       make(map[string]*redis.Client),
	}
}

// run polls replicas on `read` cadence and refreshes the replica list on
// `refresh` cadence. Returns when ctx is cancelled.
func (o *observer) run(ctx context.Context, read, refresh time.Duration) {
	o.refreshReplicas(ctx)

	readTick := time.NewTicker(read)
	defer readTick.Stop()
	refreshTick := time.NewTicker(refresh)
	defer refreshTick.Stop()

	for {
		select {
		case <-ctx.Done():
			o.closeAll()
			return
		case <-refreshTick.C:
			o.refreshReplicas(ctx)
		case <-readTick.C:
			o.readAll(ctx)
		}
	}
}

func (o *observer) refreshReplicas(ctx context.Context) {
	replicas, err := client.DiscoverReplicas(ctx, o.sentinelAddrs, o.masterName)
	if err != nil {
		log.Warn().Err(err).Msg("observer: discover replicas failed")
		return
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	seen := make(map[string]struct{}, len(replicas))
	for _, r := range replicas {
		addr := r.Name // ip:port
		seen[addr] = struct{}{}
		if _, ok := o.clients[addr]; !ok {
			o.clients[addr] = redis.NewClient(&redis.Options{
				Addr:        addr,
				DialTimeout: 2 * time.Second,
				ReadTimeout: 2 * time.Second,
			})
		}
	}
	// Drop clients for replicas that disappeared (failover demoted them away,
	// or instance rm).
	for addr, c := range o.clients {
		if _, kept := seen[addr]; !kept {
			_ = c.Close()
			delete(o.clients, addr)
			o.metrics.forgetReplica(addr)
		}
	}
	o.metrics.ReplicasObserved.Set(float64(len(o.clients)))
}

func (o *observer) readAll(ctx context.Context) {
	o.mu.Lock()
	snap := make(map[string]*redis.Client, len(o.clients))
	for k, v := range o.clients {
		snap[k] = v
	}
	o.mu.Unlock()

	var wg sync.WaitGroup
	for addr, c := range snap {
		wg.Add(1)
		go func(addr string, c *redis.Client) {
			defer wg.Done()
			start := time.Now()
			val, err := c.Get(ctx, latestKey).Result()
			dur := time.Since(start)
			o.metrics.recordReplicaRead(addr, dur, err)
			if err != nil {
				return
			}
			n, writtenAt, ok := parseValue(val)
			if !ok {
				return
			}
			o.metrics.recordReplicaValue(addr, n, writtenAt)
		}(addr, c)
	}
	wg.Wait()
}

func (o *observer) closeAll() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, c := range o.clients {
		_ = c.Close()
	}
	o.clients = nil
}
