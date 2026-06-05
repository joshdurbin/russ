# russ — Redis Cluster Simulator

`russ` provisions and manages Redis 8 Cluster installations via Docker, and runs a synthetic asynq-based e-commerce orders analytics workload against them. A writer enqueues fake orders, a processor applies them atomically through a Lua script that maintains running summaries in Redis (counters, ZSETs, HyperLogLog, leaderboards, market-basket pairs), and a REST API + Prometheus endpoint expose the analytics. The whole stack runs locally via Docker — no external dependencies beyond Docker and Go.

## Prerequisites

- Docker (running locally)
- Go 1.25+

## Build

```
make build
```

The `russ` binary is written to the repo root.

---

## Quickstart

```
russ quickstart
```

Destroys any existing russ-managed containers (no prompt) and brings up a fresh stack:

1. Observability: Prometheus + shared redis-exporter + Grafana (preloaded dashboards)
2. A 6-node Redis 8 Cluster: 3 masters + 3 replicas, named `quickstart-1`
3. The russ-client image (built if not cached)
4. A workload container running the orders-analytics writer + processor + api

| Flag | Default | Notes |
|---|---|---|
| `--cluster-name` | `quickstart-1` | Cluster name to create |
| `--masters` | `3` | Number of master nodes |
| `--replicas-per-master` | `1` | Replicas per master |

After quickstart:

```
Cluster:       quickstart-1 (3 masters + 3 replicas)
Prometheus UI: http://127.0.0.1:9090
Grafana UI:    http://127.0.0.1:3000
Client logs:   docker logs -f russ-client-quickstart-1
REST API:      http://127.0.0.1:9300/api/analytics/summary
```

---

## Cluster management

### Create a cluster

```
russ cluster create <name>
```

Starts `--masters × (--replicas-per-master + 1)` Redis 8 containers, then wires them into a Redis Cluster via `CLUSTER MEET` (using Docker network IPs for node-to-node gossip), `CLUSTER ADDSLOTSRANGE` (16384 slots divided evenly across masters), and `CLUSTER REPLICATE` (replicas assigned round-robin).

Redis Cluster requires at least 3 master nodes. The default is 3 masters + 3 replicas = 6 nodes.

Nodes announce their **container hostname** (`--cluster-announce-hostname <name>`) and use `--cluster-preferred-endpoint-type hostname` so MOVED/ASK redirects carry the container name. Inside Docker, container names resolve via Docker DNS. The host-side CLI and `russ cluster status` connect via `127.0.0.1:<port>` with an address-rewriting Dialer.

| Flag | Default | Notes |
|---|---|---|
| `--masters` | `3` | Master node count (minimum 3) |
| `--replicas-per-master` | `1` | Replicas per master (0 = no replicas, no HA) |
| `--port-range` | `6380-6500` | First-fit port allocation |
| `--max-memory` | `256mb` | Redis `maxmemory` on every node. Container cap auto-scales to `max(1 GiB, 8 × maxmemory)` |
| `--max-memory-policy` | `volatile-lru` | Eviction policy |
| `--enable-disk-persistence` | `true` | AOF + RDB + named Docker volume per node |

### List clusters

```
russ cluster ls
```

Shows each cluster's node count and `cluster_state` (ok / fail).

### Cluster status

```
russ cluster status <name>
```

Prints a subset of `CLUSTER INFO` (state, slot counts, known nodes) and the instance table derived from live `INFO replication` on each node.

```
Cluster: quickstart-1

  cluster_state:ok
  cluster_slots_assigned:16384
  cluster_slots_ok:16384
  cluster_known_nodes:6
  cluster_size:3

NAME                        ROLE       VER   PORT    REPLICATING FROM              STATUS
russ-quickstart-1-6380      master     v8    6380    -                             Up 3 minutes
russ-quickstart-1-6381      master     v8    6381    -                             Up 3 minutes
russ-quickstart-1-6382      master     v8    6382    -                             Up 3 minutes
russ-quickstart-1-6383      replica    v8    6383    172.18.0.5:6380 (up)          Up 3 minutes
russ-quickstart-1-6384      replica    v8    6384    172.18.0.6:6381 (up)          Up 3 minutes
russ-quickstart-1-6385      replica    v8    6385    172.18.0.7:6382 (up)          Up 3 minutes
```

### Remove a cluster

```
russ cluster rm <name>
```

Stops and removes all containers (and their Docker volumes if persistence was enabled).

---

## Instance management

### Add a replica

```
russ instance add <cluster-name>
```

Starts a new Redis 8 node with cluster mode enabled, joins it to the gossip ring via `CLUSTER MEET`, and assigns it as a replica of the master with the fewest current replicas via `CLUSTER REPLICATE`. The workload keeps running without interruption — replicas don't own hash slots, so adding one doesn't affect slot routing.

| Flag | Default | Notes |
|---|---|---|
| `--port-range` | `6380-6500` | First-fit port allocation |
| `--max-memory` | `256mb` | Should match the rest of the cluster |
| `--max-memory-policy` | `volatile-lru` | Eviction policy |
| `--enable-disk-persistence` | `true` | Should match the rest of the cluster |

> **Replicas only.** Adding a new master with slot rebalancing is not currently supported. Use `cluster create` to provision clusters of any size from the start.

### Remove a replica

```
russ instance rm <instance-name>
```

Issues `CLUSTER FORGET <node-id>` on all other cluster members, resets the departing node, then stops and removes the container. The cluster remains in `cluster_state:ok` as long as no master becomes slot-orphaned. Only replicas can be removed — masters own slots and cannot be removed without prior slot migration.

### List instances

```
russ instance ls <cluster-name>
```

---

## Hash slot distribution and the `{analytics}` hash tag

Redis Cluster shards data across masters by hash slot (0–16383). By default, each key hashes independently, so a multi-key command or Lua script that touches keys on different slots fails with `CROSSSLOT`.

The analytics workload's `ApplyOrder` Lua script touches 35 keys in a single `EVAL` call, and the read-path pipelines span the same keys. All analytics keys use the `{analytics}` hash tag:

```
{analytics}:orders_count
{analytics}:revenue_cents
{analytics}:top_products:units
{analytics}:hour:2026-06-05T20
...
```

Redis hashes only the content inside `{}`, so every `{analytics}:*` key maps to the same slot — whichever master owns that slot handles all analytics reads and writes. The asynq queue keys use their own hash tag (`asynq:{orders_analytics}:*`) and land on whichever master owns that different slot. Both work correctly because go-redis's `ClusterClient` and asynq route each command to the right master automatically.

---

## Client (orders-analytics workload)

`russ client workload` runs an asynq-backed e-commerce orders analytics engine inside one container per cluster. Three roles share a single Go process coordinated via `errgroup`:

| Role | What it does |
|---|---|
| writer | Wave-scaled [pond](https://github.com/alitto/pond) pool generates fake orders and enqueues `order:process` tasks on the asynq `orders_analytics` queue. Pool size oscillates between `--min-clients` and `--max-clients` over `--wave-period`. |
| processor | Asynq server consuming `orders_analytics`. Each handler runs a single Lua script that atomically applies all 35 analytics-key updates for the order in one round-trip. |
| api | `http.Server` on port 9300 serving `/api/analytics/*` JSON endpoints + Prometheus `/metrics`. |

The analytics design is **durable aggregates only** — no individual orders, customers, or line items are stored. Asynq is purely a work-distribution transport; each task is consumed and discarded once the handler returns. What lives in Redis are the running summaries under `{analytics}:*`.

### Architecture

```
russ-client-<cluster> container (on the russ Docker network)
  │
  ├─ writer ──── LPUSH ──────────────────────────────────────────┐
  │                                                              ▼
  │                                                  Redis Cluster (6 nodes)
  │                                                    {analytics}:* slot → master A
  ├─ processor ─ BRPOPLPUSH / EVALSHA ─────────────── asynq:{orders_analytics}:* slot → master B
  │
  └─ api ──────── ZREVRANGE / HGET / HMGET ──────────────────────┘
```

The container connects to cluster nodes by container name (Docker DNS resolution), so no address-rewriting Dialer is needed. The host-side `russ` binary uses a Dialer to map container names to `127.0.0.1:<port>` for its own cluster queries.

### Build the client image

```
russ client build
```

Builds `russ-client:latest` from the local source tree. After code changes, rebuild with `--force-rebuild` to pick up updated flags or logic:

```
russ client build --force-rebuild
```

| Flag | Default | Notes |
|---|---|---|
| `--force-rebuild` | `false` | Rebuild even if `russ-client:latest` already exists |
| `--project-root` | autodetect | Override path to the russ source tree |

### Run

```
russ client workload start <cluster-name>
russ client workload stop [<cluster-name>]
russ client ls
```

Writer flags:

| Flag | Default | Notes |
|---|---|---|
| `--min-clients` | `2` | Wave-trough enqueuer count |
| `--max-clients` | `50` | Wave-peak enqueuer count |
| `--wave-period` | `5m` | Full cosine cycle |
| `--min-client-ttl` | `2s` | Minimum per-enqueuer lifetime |
| `--max-client-ttl` | `30s` | Maximum per-enqueuer lifetime |
| `--tick` | `100ms` | Per-enqueuer interval; `0` = unthrottled |
| `--burst` | `false` | Run flat at `--max-clients` immediately |
| `--customer-pool` | `5000` | Pool of pre-generated customers |
| `--catalog-size` | `500` | Pre-generated product catalog |
| `--min-line-items` | `1` | Minimum line items per order |
| `--max-line-items` | `5` | Maximum line items per order |

Processor flags:

| Flag | Default | Notes |
|---|---|---|
| `--concurrency` | `100` | Asynq worker goroutines |

Subset toggles:

| Flag | Default | Notes |
|---|---|---|
| `--no-writer` | `false` | Skip the writer goroutine |
| `--no-processor` | `false` | Skip the processor goroutine |
| `--no-api` | `false` | Skip the API goroutine |

Shared:

| Flag | Default | Notes |
|---|---|---|
| `--metrics-port` | `9300` | Container-internal port for `/metrics` + JSON API |
| `--api-host-port` | `9300` | Host loopback publish port (`0` = no publish) |
| `--log-level` | `info` | russ-client log level |

### Analytics REST API

The container publishes port 9300 to host loopback by default:

| Path | Summary |
|---|---|
| `/api/analytics/summary` | Totals: orders, revenue/tax/shipping, line items, unique customers (HLL), AOV |
| `/api/analytics/top-products?by=units\|revenue&limit=N` | Top-N products |
| `/api/analytics/top-categories?by=units\|revenue&limit=N` | Top-N categories |
| `/api/analytics/by-state?by=count\|revenue&limit=N` | Orders + revenue per US state |
| `/api/analytics/timeseries?hours=N` | Per-hour rolling timeseries (1–72h) |
| `/api/analytics/value-distribution` | Order-value histogram (fixed dollar buckets) |
| `/api/analytics/top-orders/by-total` | Top 10 most expensive orders |
| `/api/analytics/top-orders/by-line-items` | Top 10 orders by cart quantity |
| `/api/analytics/top-orders/by-max-item` | Top 10 orders by highest single unit price |
| `/api/analytics/cart-size-distribution` | Histogram of items/order |
| `/api/analytics/hour-of-day` | 24-bucket UTC heatmap |
| `/api/analytics/day-of-week` | 7-bucket UTC heatmap |
| `/api/analytics/top-customers?by=spend\|orders&limit=N` | Top-N customers |
| `/api/analytics/aov-by-state?limit=N` | States ranked by average order value |
| `/api/analytics/top-zips?by=count\|revenue&limit=N` | Top-N zip codes |
| `/api/analytics/revenue-concentration` | Pareto curve over the catalog |
| `/api/analytics/top-pairs?limit=N` | Market-basket co-occurrence (top SKU pairs) |
| `/api/analytics/new-vs-returning` | Order/revenue split: first-seen vs repeat customers |
| `/metrics` | Prometheus exposition |

### Metrics

| Metric | Labels | Meaning |
|---|---|---|
| `russ_client_orders_enqueued_total` | — | Tasks successfully enqueued |
| `russ_client_orders_enqueue_errors_total` | `kind` | Enqueue failures |
| `russ_client_writer_enqueuers_active` | — | Running enqueuer goroutines |
| `russ_client_writer_wave_target` | — | Current wave target |
| `russ_client_writer_revenue_cents_enqueued_total` | — | Cumulative write-side revenue (cents) |
| `russ_client_orders_processed_total` | — | Orders fully applied |
| `russ_client_revenue_cents_processed_total` | — | Cumulative process-side revenue (cents) |
| `russ_client_line_items_processed_total` | — | Cumulative line-item quantities |
| `russ_client_processor_apply_duration_seconds` | — | Histogram: Lua round-trip latency |
| `russ_client_processor_errors_total` | `task, kind` | Task failures |
| `russ_client_api_requests_total` | `route, status` | HTTP requests by route + status class |
| `russ_client_api_request_duration_seconds` | `route` | HTTP latency by route |

### Health-check queries

```promql
# Throughput
rate(russ_client_orders_enqueued_total[1m])
rate(russ_client_orders_processed_total[1m])

# Backlog (should hover near zero)
russ_client_orders_enqueued_total - russ_client_orders_processed_total

# Lua round-trip latency
histogram_quantile(0.99, sum by (le) (rate(russ_client_processor_apply_duration_seconds_bucket[1m])))

# Errors
sum by (kind) (rate(russ_client_orders_enqueue_errors_total[1m]))
```

---

## Observability

```
russ observability start [--port 9090] [--grafana-port 3000] [--retention 2h]
russ observability ls
russ observability stop
```

Spins up three containers: Prometheus, a shared `redis_exporter` in multi-target mode, and Grafana pre-provisioned with two dashboards. Redis instances are auto-discovered via `docker_sd_config` — no restart needed when cluster nodes are added or removed.

| UI | Default URL |
|---|---|
| Prometheus | `http://127.0.0.1:9090` |
| Grafana | `http://127.0.0.1:3000` |

### Bundled Grafana dashboards

| Dashboard | What's it for |
|---|---|
| **Redis Exporter Quickstart** ([#14091](https://grafana.com/grafana/dashboards/14091)) | Per-instance: command latency, hit ratio, memory, key evictions, replication lag. Use the `instance` dropdown to select a specific node. |
| **russ-client orders analytics** (hand-built) | Workload side: enqueuers, wave target, orders/revenue throughput, Lua latency, API latency, error rates, Go runtime. |

### Useful Prometheus queries

```promql
redis_up                                                    # 1 if reachable
redis_connected_clients                                     # clients per node
redis_memory_used_bytes                                     # heap per node
rate(redis_commands_processed_total[1m])                    # ops/sec
sum by (redis_cluster) (redis_db_keys)                      # total keys per cluster
```

---

## Teardown

```
russ destroy
```

Stops and removes every russ-managed container and Docker volume, removes the `russ` network. Pass `--yes` to skip the prompt.

---

## Port ranges

| Resource | Default range |
|---|---|
| Redis cluster nodes | `6380–6500` |

Override with `--port-range` on `cluster create` or `instance add`.

---

## Container memory limits

| Container type | Hard limit |
|---|---|
| Redis node | `max(1 GiB, 8 × --max-memory)` |
| Client (workload) | 256 MiB |
| Prometheus | 512 MiB |
| Grafana | 512 MiB |
| redis-exporter | 32 MiB |

The 8× headroom on Redis nodes absorbs AOF rewrite CoW, replica output buffers (up to N × ~192 MB each), and the replication backlog. Docker caps are ceilings, not reservations — the larger value costs nothing unless transient spikes actually need it.

| `--max-memory` | Container cap |
|---|---|
| `64mb` / `128mb` | 1 GiB (floor) |
| `256mb` (default) | 2 GiB |
| `512mb` | 4 GiB |
| `1gb` | 8 GiB |

---

## Redis persistence (AOF + RDB)

Persistence is **on by default** (`--enable-disk-persistence=true`), controlled by `cluster create` and `instance add`. Each node with persistence enabled runs AOF (`--appendonly yes`, AOF rewrite trigger at 300 % growth) and RDB (Redis's default save schedule), writing into `/data` backed by a named Docker volume (`russ-data-<cluster>-<port>`). The volume is removed automatically when the container is torn down via any russ path.

Pass `--enable-disk-persistence=false` for a pure in-memory setup (no AOF, no RDB, no Docker volume). Diskless RDB sync between masters and replicas still works — that's a memory-to-socket stream, not a disk write.

When adding instances to an existing cluster, match the persistence setting of the rest of the cluster; russ doesn't auto-detect it.

---

## Replication stability tuning

Every Redis node is started with these non-default flags:

- `--client-output-buffer-limit replica 192mb 48mb 60` — per-replica output buffer cap. Balances two failure modes: too tight (64MB/16MB) kicked replicas under sustained writes; too loose (Redis default 256MB/64MB) let N replicas catching up after a failover collectively approach the container cap.
- `--repl-backlog-size 32mb` — larger partial-resync backlog reduces full-resync frequency on brief replica blips.
- `--repl-diskless-sync yes` — full-sync RDB streams memory-to-socket, bypassing the master-side output buffer. Removes the output-buffer pressure that scales with dataset size under disk-based sync.
- `--repl-diskless-sync-delay 0` — sync begins on first PSYNC; any positive delay pre-fills the output buffer with writes-during-wait, which can kick the replica before the sync starts.
- `--auto-aof-rewrite-percentage 300` (persistence on only) — raises the AOF rewrite trigger from 100 % (doubles) to 300 % to prevent back-to-back rewrite forks stacking with the BGSAVE-for-SYNC fork during post-failover catch-up storms.

All flags are visible via `redis-cli -p <port> CONFIG GET <key>`.

---

## Verification reference

| What to check | Command |
|---|---|
| Cluster health + instance topology | `russ cluster status <cluster>` |
| All clusters | `russ cluster ls` |
| Instances in a cluster | `russ instance ls <cluster>` |
| Client containers | `russ client ls` |
| Observability stack | `russ observability ls` |
| Live metrics | `http://127.0.0.1:9090` (Prometheus) · `http://127.0.0.1:3000` (Grafana) |
| Analytics API | `http://127.0.0.1:9300/api/analytics/summary` |
| Raw cluster topology | `redis-cli -p <port> CLUSTER NODES` |
| Slot assignment | `redis-cli -p <port> CLUSTER INFO` |
| Replication state of a node | `redis-cli -p <port> INFO replication` |
