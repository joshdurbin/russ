# russ — Redis Upgrade Sentinel Simulator

`russ` manages Redis and Redis Sentinel containers via Docker to simulate and validate the upgrade path from Redis 6.x to 8.x under sentinel-managed replication. It can also run synthetic load (writer + reader) against the cluster during the upgrade to observe behavior under traffic.

## Prerequisites

- Docker (running locally)
- Go 1.25+

## Build

```
make build
```

The `russ` binary is written to the repo root.

---

## Upgrade path overview

```
AllV6 → MixedVersions → FailoverComplete → ReplicationBroken → V6Destroyed → V8SentinelsAdded → Done
```

Each step below maps to one or more `russ` commands. `russ cluster status <cluster>` shows the current lifecycle state and the next valid trigger; `russ cluster lifecycle <cluster>` renders the full FSM as a tree with the current position marked (the topology is introspected from the actual state machine in `internal/state/machine.go`, so the tree stays in sync if the FSM is ever extended).

The FSM deliberately has no `AllV6 → FailoverComplete` shortcut. A failover is allowed in any state (see [Operational failovers](#operational-failovers)), but the lifecycle only *advances* on a failover when the cluster is in `MixedVersions`, so a no-v8 cluster can't accidentally walk into `V6Destroyed`.

---

## Step-by-step walkthrough

### 0. Bootstrap v6 sentinels

```
russ sentinel bootstrap --version=6 --count=3
```

Builds the tilt-patched sentinel image (compiled from Redis source, tag `russ-sentinel:6`) if it doesn't already exist, then starts an odd number of sentinel containers. Three is the minimum for a meaningful quorum.

Useful flags:

| Flag | Default | Notes |
|---|---|---|
| `--version` | `6` | `6` or `8`. v8 bootstrap also reconciles existing clusters in `V6Destroyed`. |
| `--count` | `3` | Odd numbers recommended. |
| `--port-range` | `26379-26450` | First-fit allocation. |
| `--redis-source-version` | `6.2.17` (v6) / `8.0.0` (v8) | Exact tag compiled into the sentinel image. |
| `--force-rebuild` | `false` | Force rebuild of the sentinel image. |

Confirm:

```
russ sentinel ls
```

---

### 1. Create a v6 cluster

```
russ cluster create mycluster --version=6 --count=3
```

Starts one master and `--count - 1` replicas, configures replication, and registers the cluster with every running sentinel via `SENTINEL MONITOR`. State file `~/.russ/state/mycluster.json` is created at `AllV6`.

Confirm:

```
russ cluster status mycluster
```

The output uses live state from each node (see [Reading the instance table](#reading-the-instance-table)). Expect one `master` and two `replica` rows, all showing `REPLICATING FROM` pointing at the master.

---

### 2. (Optional) Start the client health probe

If you want a live indicator of cluster + replication health during the upgrade, build the client image once and start a client container for the cluster. See [Client (cluster health probe)](#client-cluster-health-probe) for the full reference.

```
russ client build
russ client workload start mycluster
```

Tail logs:

```
docker logs -f russ-workload-mycluster
```

Combined with `russ observability start` (see [Observability](#observability)), Prometheus auto-discovers the workload and you can watch the upgrade play out against PromQL queries.

---

### 3. Add a v8 replica

```
russ instance add mycluster --version=8
```

Starts a new instance, points it at the current sentinel-reported master via `REPLICAOF`, and waits until replication is fully synced. The command then issues `SENTINEL RESET` and polls `SENTINEL REPLICAS` until at least one sentinel has acknowledged the new replica — this closes the race where a failover triggered immediately afterward could otherwise strand the new node in a chained replication state.

Confirm:

```
russ cluster status mycluster
```

Expected state: `MixedVersions`. Four instances: three v6, one v8 replica. The v8 row should show `REPLICATING FROM <master>:<port> (up)`.

> Add more v8 replicas with repeated `instance add --version=8` calls if you want additional candidates before failover.

---

### 4. Fail over to a v8 master

```
russ sentinel failover mycluster
```

The command does four things in order:

1. **Pre-flight reconciliation.** Walks every Redis instance and re-issues `REPLICAOF` against the current sentinel-reported master on any node that isn't already a direct replica of it. Then waits for sentinel to see the rewired replica via its natural `INFO REPLICATION` poll cycle. This breaks chained replication so no v8 candidate is invisible to sentinel.
2. **Priority adjustment.** `replica-priority 1` on v8 replicas, `100` on v6 replicas, biasing sentinel's election toward a v8 node. The current master per sentinel is identified live (not from stale container labels) and skipped.
3. **Priority propagation wait.** Sentinel's view of `replica-priority` is cached and only refreshes on its periodic `INFO REPLICATION` poll (~10s default). If we triggered FAILOVER immediately, sentinel would still see the default priority of 100 across the board and pick the winner by offset/runid tiebreak — frequently a v6. The command polls `SENTINEL REPLICAS` until sentinel has actually observed the new priorities on every non-master node (up to 30s).
4. **`SENTINEL FAILOVER`.** Retries up to 6 times with 5s backoff if sentinel briefly returns `NOGOODSLAVE` (it can while still pinging freshly-discovered replicas). Then waits up to 60s for the master to switch to any v8 port — all v8 candidates have equal priority, so the winner is chosen by replication offset and runid; the wait accepts whichever wins.

Expected state: `FailoverComplete`. `russ cluster status mycluster` should show the v8 instance as `master` and the v6 instances as `replica` rows pointing at it.

---

### 5. Isolate v6 replicas

```
russ cluster isolate-v6-replicas mycluster
```

Issues `REPLICAOF NO ONE` on every v6 instance that is not the current master (after step 4, that's all of them) and then issues `SENTINEL RESET mycluster` so sentinels rediscover the topology.

Expected state: `ReplicationBroken`. In `russ cluster status mycluster` the v6 rows will now show role `isolated` with `REPLICATING FROM` set to `-` — they consider themselves masters but sentinel disagrees.

---

### 6. Remove v6 instances

```
russ instance ls mycluster        # find the v6 instance names
russ instance rm russ-mycluster-<port>
russ instance rm russ-mycluster-<port>
russ instance rm russ-mycluster-<port>
```

Each removal issues `REPLICAOF NO ONE`, resets all sentinels, then stops and removes the container. When the last v6 instance is removed the lifecycle advances automatically.

Expected state after the last removal: `V6Destroyed`. Only the v8 instances should remain.

---

### 7. Bootstrap v8 sentinels

```
russ sentinel bootstrap --version=8 --count=3
```

The bootstrap with `--version=8` triggers extra behavior:

1. **Pre-flight warning.** Lists any clusters not yet in `V6Destroyed` (those won't be registered with the new sentinels).
2. **Builds the v8 sentinel image** (`russ-sentinel:8`) if needed.
3. **Starts the new sentinels.**
4. **Registers `V6Destroyed` clusters** with each new sentinel by asking a still-running v6 sentinel for the current master, then issuing `SENTINEL MONITOR` on the new ones.
5. **Recalculates quorum** (`floor(N/2) + 1`) and pushes it to every sentinel.
6. **Advances the FSM** on every cluster currently in `V6Destroyed` to `V8SentinelsAdded`.

Confirm: `russ sentinel ls` shows six sentinels (three v6, three v8); state is `V8SentinelsAdded`.

---

### 8. Remove v6 sentinels

```
russ sentinel ls
russ sentinel rm russ-sentinel-<port>
russ sentinel rm russ-sentinel-<port>
russ sentinel rm russ-sentinel-<port>
```

Each removal:

1. Issues `SENTINEL REMOVE` for every cluster the sentinel was monitoring.
2. Recalculates quorum across the remaining sentinels.

When the last v6 sentinel is removed, every cluster in `V8SentinelsAdded` advances to `Done`. Confirm with `russ cluster status mycluster`.

---

## Reading the instance table

Both `russ cluster status <cluster>` and `russ instance ls <cluster>` render the same table, derived from each node's *live* `INFO replication` (not from container labels):

```
NAME              ROLE       VER  PORT   REPLICATING FROM           STATUS
russ-mc-6380      replica    v6   6380   russ-mc-6384:6384 (up)     Up 5 minutes
russ-mc-6381      replica    v6   6381   russ-mc-6384:6384 (up)     Up 5 minutes
russ-mc-6382      isolated   v6   6382   -                          Up 5 minutes
russ-mc-6383      replica    v8   6383   russ-mc-6384:6384 (up)     Up 4 minutes
russ-mc-6384      master     v8   6384   -                          Up 4 minutes
russ-mc-6385      replica    v8   6385   russ-mc-6384:6384 (down)   Up 4 minutes
```

`ROLE` values:

| Value | Meaning |
|---|---|
| `master` | Node reports `role:master` and sentinel agrees this is the cluster leader. |
| `replica` | Node reports `role:slave`. |
| `isolated` | Node reports `role:master` but sentinel reports a *different* port as master — typically after `REPLICAOF NO ONE`, or stale after a failover. |
| `unreachable` | `INFO replication` failed against the node. |

`REPLICATING FROM` is `<master_host>:<master_port> (<master_link_status>)` for replicas, or `-` for masters / isolated nodes. A `(down)` status flags a broken replication link. A target that doesn't match the current leader flags chained replication.

---

## Client (cluster health probe)

`russ client workload` runs three concurrent roles inside one container per cluster, doing nothing fancier than `INCR` + `SET` + `GET` against the sentinel-resolved topology. The whole point is to prove the cluster is healthy and quantify replication behavior — not to test command-level upgrade compatibility.

| Role | Connection | What it does | Cadence |
|---|---|---|---|
| writer | failover client (master) | `INCR russ:client:counter` → `n`, then `SET russ:client:latest "<n>:<unix-nano>"` | 200 ms |
| master reader | failover client (master) | `GET russ:client:latest`, parse `n` and write-time | 200 ms |
| replica observer | direct connections to every replica returned by `SENTINEL REPLICAS` | `GET russ:client:latest` against each replica, parse the same way | 200 ms (read), 10 s (replica-list refresh) |

All three commands are Redis 6.2-compatible (and were already there long before). The writer maintains a monotonic counter plus a wall-clock timestamp on the master; the readers verify the counter advances and quantify how stale each replica's view is.

### Build the client image

```
russ client build
```

Constructs a multi-stage build context from the local source tree (auto-detected by walking up from the cwd looking for `module github.com/bigcommerce/russ`) and builds `russ-client:latest`.

| Flag | Default | Notes |
|---|---|---|
| `--force-rebuild` | `false` | Rebuild even if `russ-client:latest` already exists. Reuses Docker's layer cache so offline rebuilds work when only source files changed. To truly bypass the cache, `docker image rm russ-client:latest` first. |
| `--project-root` | autodetect | Override the path to the russ source tree. |

### Run

```
russ client workload start mycluster
russ client workload stop [mycluster]   # stop one cluster's client, or all
russ client ls                           # list client containers
```

| Flag | Default | Notes |
|---|---|---|
| `--interval` | `200ms` | Cadence at which writer/reader/observer each tick |
| `--refresh-interval` | `10s` | How often the observer re-asks sentinel for the replica list |
| `--metrics-port` | `9300` | /metrics port inside the russ network (Prometheus scrapes via docker_sd) |
| `--log-level` | `info` | russ-client log level |

One container per cluster: `russ-workload-<cluster>`. Spin up more for additional clusters in parallel.

### Metrics

The client binary embeds the Prometheus client and exposes `/metrics` on port 9300 inside the russ network. Prometheus picks the container up automatically (it's labeled `russ.role=client-workload` + `russ.target.port=9300`).

**Writer** (master accepts writes?):

| Metric | Labels | Meaning |
|---|---|---|
| `russ_client_writes_total` | `status` | Writer ops, `status` = `ok`/`error` |
| `russ_client_write_duration_seconds` | — | Histogram of writer-op latency |
| `russ_client_write_errors_total` | `kind` | Classified failures |

**Master reader** (master serves reads, and writes are landing):

| Metric | Labels | Meaning |
|---|---|---|
| `russ_client_master_reads_total` | `status` | Master GETs |
| `russ_client_master_read_duration_seconds` | — | Histogram |
| `russ_client_master_read_errors_total` | `kind` | Classified failures |
| `russ_client_master_value` | — | Latest counter value the master returned |
| `russ_client_master_value_age_seconds` | — | Seconds since the master's current value was originally written. Spikes if the writer stalls. |

**Replica observer** (followers reachable, and tracking the leader):

| Metric | Labels | Meaning |
|---|---|---|
| `russ_client_replicas_observed` | — | Count of replicas the observer is currently polling |
| `russ_client_replica_reads_total` | `replica, status` | Per-replica GETs |
| `russ_client_replica_read_duration_seconds` | `replica` | Histogram |
| `russ_client_replica_read_errors_total` | `replica, kind` | Classified failures |
| `russ_client_replica_value` | `replica` | Counter value the replica returned |
| `russ_client_replica_value_age_seconds` | `replica` | Seconds since the master originally wrote what this replica is now showing |
| `russ_client_replica_lag_count` | `replica` | `master_value - replica_value` (positive = replica behind by N writes; small positive/negative noise is normal due to scrape timing) |

Plus the standard `process_*` and `go_*` collectors.

Error `kind`s (small, stable set):

| `kind` | Trigger |
|---|---|
| `readonly` | Op routed to a replica during failover |
| `loading` | Replica still loading data from the master |
| `master_down` | Sentinel-driven during a failover window |
| `timeout` | Network-level timeout |
| `network` | Connection refused / EOF / broken pipe |
| `other` | Anything else |

`redis.Nil` (key-not-found) is not a failure and counts as `status=ok`.

### Health-check queries

```
# Is the leader accepting writes?
rate(russ_client_writes_total{status="ok"}[1m])
    # Should be steady at 1 / interval — i.e. ~5/s with default 200ms cadence

# Is the leader serving reads?
rate(russ_client_master_reads_total{status="ok"}[1m])
    # Same expected rate

# Is every follower reachable?
sum by (replica) (rate(russ_client_replica_reads_total{status="ok"}[1m]))
    # One row per replica, all at the same rate

# Is replication catching up?
russ_client_replica_lag_count
    # Should hover near zero. Sustained positive value = replica is N writes behind.

# How stale is each replica's view, in seconds?
russ_client_replica_value_age_seconds
    # Should track master_value_age_seconds closely. Divergence = replication lag in time.

# Failover transition errors
sum by (kind) (rate(russ_client_write_errors_total{kind=~"readonly|master_down|loading"}[1m]))
sum by (kind, replica) (rate(russ_client_replica_read_errors_total[1m]))
    # Spikes briefly during failovers; should settle to zero afterward
```

The `workload_cluster` label (set by Prometheus's relabel rules) lets you slice by cluster when multiple client containers are running in parallel.

---

## Observability

`russ observability` spins up a Prometheus container plus **one shared** `redis_exporter` running in multi-target mode. The exporter has no per-instance configuration — it exposes `/scrape?target=redis://...` and dials whatever URI Prometheus passes in. Prometheus discovers Redis containers via `docker_sd_config` and rewrites each scrape to flow through the shared exporter. Adding or removing Redis instances is auto-discovered within ~5 seconds; no exporter sidecars to manage, no Prometheus reload.

```
russ observability start [--retention 2h] [--port 9090]   # Prometheus + shared redis-exporter
russ observability ls                                      # show what's running
russ observability stop                                    # tear down
```

Prometheus's UI is published on `127.0.0.1:9090` (override with `--port`). Default retention is 2 hours; configurable via `--retention` (e.g. `30m`, `1d`).

### How the dynamic wiring works

The shared exporter is `russ-redis-exporter` on the russ network. It's started with no `REDIS_ADDR` so it stays in multi-target mode. Prometheus's `redis-instances` scrape job:

1. Uses `docker_sd_configs` against `unix:///var/run/docker.sock` (read-only bind mount) filtered to `russ.managed=true`.
2. Relabel-keeps only containers with `russ.role=master|replica` (skips sentinels, the exporter itself, workload containers).
3. Relabel-keeps only the russ-network entry per container, so multi-network containers don't emit duplicate targets.
4. Builds the target URI from the container's existing `russ.name` and `russ.port` labels: `redis://<name>:<port>`.
5. Stores that URI in `__param_target` and `instance`, then **overrides `__address__` to `russ-redis-exporter:9121`**.
6. Promotes `russ.name` / `russ.cluster` / `russ.version` to `redis_instance` / `redis_cluster` / `redis_version_major`.

Net effect: every scrape becomes `GET http://russ-redis-exporter:9121/scrape?target=redis://<container-name>:<port>`, the exporter dials that Redis on demand, and Prometheus stores the result with stable instance/cluster/version labels.

Container DNS on the russ bridge resolves all the `russ-...` names, so the exporter never needs to know the IP of anything.

### Useful queries

```
redis_up                                           # 1 if reachable, 0 otherwise
redis_connected_clients                            # clients per instance
redis_memory_used_bytes                            # heap usage
redis_db_keys                                      # key count per db
rate(redis_commands_processed_total[1m])           # ops/sec per instance
redis_master_link_up                               # replication link health
redis_master_repl_offset - redis_slave_repl_offset # replication lag
redis_up{redis_version_major="8"}                  # v8-only filter during upgrade
sum by (redis_cluster) (redis_db_keys)             # total keys per cluster
```

The exporter (`oliver006/redis_exporter`) publishes ~70 redis-specific metrics — see [its README](https://github.com/oliver006/redis_exporter#whats-exported) for the full list.

### Notes

- Prometheus runs as `root` inside the container so it can read `/var/run/docker.sock` regardless of which group owns the socket inside the Docker VM. Fine for local dev; never appropriate for production.
- The config file is materialized at `~/.russ/prometheus/prometheus.yml` and bind-mounted read-only into the container. Edit it there and run `curl -X POST http://127.0.0.1:9090/-/reload` if you need to tweak scrape behavior without a restart.
- `russ observability start` cleans up any legacy per-instance exporters from earlier versions before bringing up the shared one.
- `russ destroy` sweeps the Prometheus and exporter containers along with everything else (they're labeled `russ.managed=true`).

---

## Operational failovers

`russ sentinel failover <cluster>` is safe to run in any lifecycle state, not just `MixedVersions`. Useful before any upgrade work — to confirm the cluster fails over cleanly under load, exercise sentinel quorum behavior, etc. The pre-flight reconciliation step still runs (so chained replicas get healed), but the FSM only advances when the cluster is in `MixedVersions`. From any other state the command prints:

```
Note: cluster is in state <X>; failover will run but upgrade state will not advance.
```

---

## Teardown

```
russ destroy
```

Stops and removes every russ-managed container (cluster nodes, sentinels, and client containers), removes the `russ` Docker network, and deletes all persisted upgrade-state files under `~/.russ/state/`. Pass `--yes` to skip the confirmation prompt.

---

## Verification reference

| What to check | Command |
|---|---|
| Current upgrade state | `russ cluster status <cluster>` |
| FSM tree with current position | `russ cluster lifecycle <cluster>` |
| Live role + replication target per instance | `russ instance ls <cluster>` |
| All sentinels | `russ sentinel ls` |
| All client containers | `russ client ls` |
| Observability stack status | `russ observability ls` |
| Live metrics for any instance | `http://127.0.0.1:9090` (PromQL UI) |
| What a sentinel thinks the master is | `redis-cli -p <sentinel-port> SENTINEL MASTER <cluster>` |
| All clusters known to a sentinel | `redis-cli -p <sentinel-port> SENTINEL MASTERS` |
| Replicas known to a sentinel | `redis-cli -p <sentinel-port> SENTINEL REPLICAS <cluster>` |
| Raw replication state of an instance | `redis-cli -p <instance-port> INFO replication` |

---

## Quorum behavior

Quorum is recalculated as `floor(N/2) + 1` whenever the sentinel fleet size changes:

| Sentinels | Quorum |
|---|---|
| 3 | 2 |
| 6 (3 v6 + 3 v8 during step 7) | 4 |
| 5 (after first v6 removal) | 3 |
| 4 | 3 |
| 3 (v8 only, upgrade complete) | 2 |

`russ` pushes the updated quorum to all affected sentinels via `SENTINEL SET <cluster> quorum <n>` automatically.

---

## Port ranges

| Resource | Default range |
|---|---|
| Redis instances | `6380–6500` |
| Sentinels | `26379–26450` |

Override with `--port-range` on any command that allocates ports. Client containers don't bind ports on the host — they connect outbound to sentinels on the russ network.

---

## Container memory limits

Per-container memory caps are applied automatically so ctop and `docker stats` show realistic numbers (Docker otherwise defaults each container's limit to the host VM's full memory):

| Container type | Hard limit |
|---|---|
| Redis instance | 128 MiB |
| Sentinel | 64 MiB |
| Client (workload) | 256 MiB |

These are independent of Redis's `--maxmemory` setting (the dataset eviction trigger). The container limit is the outer process-level cap.

---

## State files

Per-cluster upgrade state lives in `~/.russ/state/<cluster>.json`. This is the persistence layer behind `russ cluster status`. Removing a cluster with `russ cluster rm` or `russ destroy` cleans up the corresponding state file; an orphaned state file (cluster containers gone, JSON left behind) shows up as `cluster not found` on cluster-scoped commands and can be removed manually:

```
rm ~/.russ/state/<cluster>.json
```
