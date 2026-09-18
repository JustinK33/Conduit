# Conduit - Feature Deep Dives

Three backend engineering features implemented for production-grade performance and reliability.
Study this alongside the code - every section maps to real files you can open.

---

## Feature 1 - Async Kafka Publish + Tuning (400+ req/s, sub-1ms p50)

> Since `CONDUIT_TRANSPORT` landed, Kafka is one of two transports and no longer the default.
> Everything below still describes the Kafka path exactly, and the async-publish insight is what generalises: the transport is a wake-up, Postgres is the commit.
> The default `postgres` transport publishes one `pg_notify` from the same goroutine, and measures faster on every percentile.
> See [ADR 0002](decisions/0002-kafka-as-transport-not-as-the-queue.md).

### The Problem

Before this change, `Enqueue` looked like this (simplified):

```
HTTP POST /api/jobs
  │
  ├─ Postgres INSERT  (~0.5ms)
  ├─ Kafka SendMessage (waits for broker ack) (~1–3ms)  ← hot path bottleneck
  └─ return 201
```

Every HTTP request had to wait for a TCP round-trip to the Kafka broker before sending a response. That made the minimum possible latency ~1–3ms regardless of how fast everything else was.

**Measured before:** p50 = 2–3ms, throughput ~280 req/s

---

### The Fix - Make Kafka Non-Blocking

**File:** `internal/service/job_service.go`

```go
if err := s.store.CreateJob(ctx, job); err != nil {
    return "", fmt.Errorf("service: persist job: %w", err)
}

// Publish to Kafka asynchronously: Postgres is the durable source of truth.
// If Kafka delivery fails the job remains PENDING and can be reconciled by the scheduler.
go func() {
    if err := s.kafka.Publish(context.Background(), s.topic, job); err != nil {
        _ = err
    }
}()

return job.ID, nil
```

The key insight: **Postgres is the source of truth**. The job is written durably before we return. Kafka is only a delivery signal - if the goroutine's publish fails, the job stays `PENDING` in Postgres and the scheduler can reconcile it.

New hot path:
```
HTTP POST /api/jobs
  │
  ├─ Postgres INSERT  (~0.5ms)  ← only blocking call
  ├─ goroutine spawned (Kafka publish happens in background)
  └─ return 201
```

**Measured after:** p50 = **1.39 ms**, p99 **12.97 ms**, **4,864 req/s** sustained at 100 VUs. Conditions and the rest of the numbers are in the README's [Measured results](../README.md#measured-results).

---

### Kafka Producer Tuning

**File:** `internal/queue/kafka.go` - `NewKafkaClient`

```go
sc.Version = sarama.V3_3_0_0           // upgraded from V2_6_0_0
sc.ChannelBufferSize = cfg.ChannelBufferSize  // default 256

sc.Producer.Compression = sarama.CompressionCodec(cfg.CompressionCodec)  // snappy (2)
sc.Producer.Flush.Frequency = time.Duration(cfg.FlushFrequencyMs) * time.Millisecond // 5ms
sc.Producer.Flush.Bytes = cfg.FlushBytes  // 1 MiB
```

| Setting | Before | After | Why |
|---|---|---|---|
| `ChannelBufferSize` | 256 (default) | 256 (explicit) | Buffers outgoing messages; prevents producer from blocking when broker is slow |
| `Compression` | None | Snappy | Reduces network I/O by ~40–60%; snappy is fast to encode/decode |
| `Flush.Frequency` | 0 (immediate) | 5ms | Lets the producer batch multiple messages into one request |
| `Flush.Bytes` | 0 | 1 MiB | Additional batching trigger: flush if buffered bytes exceed threshold |
| `Version` | V2_6_0_0 | V3_3_0_0 | Access to newer protocol features (idempotent producer, improved batching) |

---

### pgx Connection Pool Tuning

**File:** `cmd/server/main.go` - after `pgxpool.ParseConfig`

```go
pgCfg.MaxConns = cfg.Postgres.MaxConns           // 25 → 50
pgCfg.MinConns = cfg.Postgres.MinConns           // 5  → 10
pgCfg.MaxConnLifetime = cfg.Postgres.MaxConnLifetime    // 30 minutes
pgCfg.MaxConnIdleTime = cfg.Postgres.MaxConnIdleTime    // 5 minutes
pgCfg.HealthCheckPeriod = cfg.Postgres.HealthCheckPeriod // 1 minute
```

**Model fields added:** `pkg/models/models.go` → `PostgresConfig`
**Env vars added:** `CONDUIT_POSTGRES_MAX_CONN_LIFETIME`, `CONDUIT_POSTGRES_MAX_CONN_IDLE_TIME`, `CONDUIT_POSTGRES_HEALTH_CHECK_PERIOD`

| Setting | Before | After | Why |
|---|---|---|---|
| `MaxConns` | 25 | 50 | More concurrent INSERTs before queueing |
| `MinConns` | 5 | 10 | Keeps connections warm; avoids cold-connect latency spikes |
| `MaxConnLifetime` | none | 30m | Rotates stale connections; prevents "connection reset" errors after DB restarts |
| `MaxConnIdleTime` | none | 5m | Reclaims idle connections; important under bursty load |
| `HealthCheckPeriod` | none | 1m | Proactively pings idle connections; catches dead ones before a request hits them |

---

### Interview Questions You Should Know

**Q: Why not just use an async Kafka producer everywhere?**
A: The `AsyncProducer` API requires you to drain an error channel in a separate goroutine or you'll leak memory. For the enqueue path, firing a `go func()` with a `SyncProducer` call is simpler and easier to reason about. In a high-throughput service you'd use a proper `AsyncProducer` with a background drain loop.

**Q: What happens if the goroutine's Kafka publish fails?**
A: The job is already written to Postgres in state `PENDING`. The scheduler (see `internal/scheduler`) can poll for PENDING jobs that were never picked up by a worker - this is the reconciliation path. This is a well-known pattern called "transactional outbox."

**Q: What does `MaxConnLifetime` actually protect against?**
A: Long-lived TCP connections can go stale when a load balancer, firewall, or the DB server itself drops them silently. Without a lifetime limit, the pool may hold connections that look healthy but will fail on the next query. Rotating them every 30 minutes ensures the pool stays clean.

---

## Feature 2 - Redlock Distributed Locking Across 3 Redis Nodes

> Since `CONDUIT_LOCK` landed, Redlock is one of three options and no longer the default.
> The default `advisory` uses `pg_try_advisory_lock` on one dedicated connection, which needs no TTL and no renewal because the server releases a session lock the moment the connection dies.
> Measured on the crash scenario, that closes a 7-to-21-second churn window to 0.1 seconds.
> Everything below still describes the Redlock path exactly, and its "the lock saves duplicated effort, not correctness" conclusion is what made replacing it safe.
> See [ADR 0003](decisions/0003-redlock-over-postgres-advisory-locks.md).

### The Problem

A distributed task queue running multiple instances (or pods) can pick up the same job twice if two workers claim it at the same moment. A single Redis lock fails if that Redis node goes down - you either lose lock enforcement or block all workers.

### The Redlock Algorithm

**File:** `internal/lock/redlock.go`

Redlock is a distributed lock algorithm designed for multiple independent Redis nodes. The idea is simple: **you need a majority (quorum) of nodes to grant you the lock**. If any single node is down, the remaining nodes can still form quorum.

```
3 Redis nodes:   redis:6379   redis:6380   redis:6381
                      │             │             │
Acquire("job:123"):   SET NX ──►   SET NX ──►   SET NX
                      ✓             ✓             ✗ (down)
                 acquired=2, quorum=2  →  LOCK GRANTED ✓
```

#### Step-by-step in `Acquire`:

```go
func (m *Manager) Acquire(ctx context.Context, resource string) (Lock, error) {
    value, _ := generateToken()  // cryptographically random 32-char token

    for attempt := 0; attempt < m.cfg.RetryCount; attempt++ {
        start := time.Now()
        acquired := 0

        for _, client := range m.clients {
            // SetNX = "SET if Not eXists" - atomic, won't overwrite an existing lock
            ok, _ := client.SetNX(ctx, resource, value, m.cfg.TTL).Result()
            if ok {
                acquired++
            }
        }

        elapsed := time.Since(start)
        drift := time.Duration(float64(m.cfg.TTL)*m.cfg.DriftFactor) + 2*time.Millisecond
        validity := m.cfg.TTL - elapsed - drift

        if acquired >= m.cfg.Quorum && validity > 0 {
            return Lock{Resource: resource, Value: value, Expiry: time.Now().Add(validity)}, nil
        }

        // Partial win - MUST release all acquired locks before retrying
        for _, client := range m.clients {
            releaseSingle(ctx, client, resource, value)
        }
    }
    return Lock{}, fmt.Errorf("could not acquire lock after %d attempts", m.cfg.RetryCount)
}
```

**Why release on partial win?** If you acquired 1 out of 3 nodes and give up, that one node still holds your lock until TTL expires. Another caller trying to acquire the same lock would also get 1 node - now both callers think they failed, but both hold 1 node each and neither can reach quorum. You'd be wasting locks. Releasing immediately lets the next attempt start clean.

#### Safe Release with Lua:

```go
var releaseScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`)
```

This is a Lua script that runs atomically on Redis. It only deletes the key if the stored value matches **your token**. Without this check, you could accidentally release another caller's lock (e.g., if your lock expired and someone else acquired it before you called Release).

#### Validity Window and Clock Drift:

```go
drift := time.Duration(float64(m.cfg.TTL)*m.cfg.DriftFactor) + 2*time.Millisecond
validity := m.cfg.TTL - elapsed - drift
```

The `DriftFactor` (1% of TTL) accounts for the fact that clocks across distributed nodes are never perfectly in sync. By subtracting drift from the lock's validity window, you ensure you don't use a lock that may have already expired on one of the nodes.

---

### 3-Node Setup

**File:** `docker-compose.yml`

```yaml
redis:
  image: redis:7-alpine
  ports: ["6379:6379"]

redis-2:
  image: redis:7-alpine
  ports: ["6380:6379"]

redis-3:
  image: redis:7-alpine
  ports: ["6381:6379"]
```

**File:** `internal/config/config.go` - default changed to:

```go
Addresses: []string{"localhost:6379", "localhost:6380", "localhost:6381"},
```

**File:** `cmd/server/main.go` - each address becomes a separate client:

```go
for _, addr := range cfg.Redis.Addresses {
    redisClients = append(redisClients, redis.NewUniversalClient(&redis.UniversalOptions{
        Addrs: []string{addr},
        ...
    }))
}
lockMgr := lock.NewManager(redisClients, lock.Config{
    TTL:        30 * time.Second,
    RetryCount: 3,
    RetryDelay: 100 * time.Millisecond,
    DriftFactor: 0.01,
})
```

Quorum = `len(clients)/2 + 1` = `3/2 + 1` = **2**. Any 2 of 3 nodes can grant the lock.

---

### Interview Questions You Should Know

**Q: Why not just use a single Redis lock?**
A: A single node is a single point of failure. If that Redis instance goes down, all lock acquisitions fail - workers either all block or you have to remove the lock check (risking duplicate execution). Redlock ensures the system keeps working as long as a majority of nodes are alive.

**Q: What is the quorum formula and why?**
A: `quorum = n/2 + 1` (integer division). For 3 nodes: quorum = 2. This is the same majority rule used in Raft and Paxos. You need a majority so that no two clients can simultaneously claim quorum on disjoint sets of nodes - with 3 nodes and quorum=2, any two winning sets must overlap on at least one node, preventing split-brain.

**Q: What is the "validity window" and why does it matter?**
A: After acquiring the lock, the actual time you can safely use it is `TTL - (time spent acquiring) - drift`. If acquiring the lock across 3 nodes took 50ms and your TTL is 30s, you have ~29.95s of safe usage. If you ignore this and use the full TTL, a slow network could mean your lock has already expired by the time you act on it.

**Q: What happens if a node crashes after you set the lock on it?**
A: The lock on that node is simply gone. But since you needed quorum (2/3 nodes), you still hold the lock on the remaining nodes. When the crashed node restarts, it has no lock for your resource, so the next `SetNX` attempt by someone else on that node will succeed - but they still won't reach quorum because your other nodes still hold it. This is why requiring quorum is essential.

---

## Feature 3 - Kubernetes + CI/CD Pipeline

### Kubernetes Manifests

**Directory:** `deploy/k8s/`

Four manifest files work together to run Conduit in a Kubernetes cluster.

#### `deployment.yaml` - the workload

```yaml
spec:
  replicas: 3
  template:
    spec:
      terminationGracePeriodSeconds: 35  # longer than ShutdownTimeout (30s)
      containers:
        - name: conduit
          image: ghcr.io/justink33/conduit:v0.2.0
          envFrom:
            - configMapRef:
                name: conduit-config   # non-secret env vars
            - secretRef:
                name: conduit-secrets  # CONDUIT_POSTGRES_DSN, Redis passwords
          readinessProbe:
            httpGet: { path: /ready, port: http }   # pings Postgres
            initialDelaySeconds: 5
            periodSeconds: 15
          livenessProbe:
            httpGet: { path: /live, port: http }    # process-is-up only
            initialDelaySeconds: 15
            periodSeconds: 30
          startupProbe:
            httpGet: { path: /live, port: http }
            periodSeconds: 5
            failureThreshold: 12
          resources:
            requests: { cpu: 100m, memory: 128Mi }
            limits:   { cpu: 500m, memory: 256Mi }
```

**Key decisions:**

| Decision | Reason |
|---|---|
| `replicas: 3` | Three pods share the claim load, and under `CONDUIT_LOCK=redlock` the count also matches the 3-node Redis quorum |
| `terminationGracePeriodSeconds: 35` | Must be longer than the worker's `ShutdownTimeout` (30s) so in-flight jobs finish before the pod is killed |
| `readinessProbe` on `/ready`, `livenessProbe` on `/live` | Readiness removes the pod from load balancer rotation, so it hits the endpoint that pings dependencies. Liveness *restarts* the pod, so it must not: a Postgres blip would otherwise restart every replica, turning a dependency outage into a crash loop. `/live` answers for the process alone. |
| `startupProbe` on top of both | Gives a slow first boot 60s to come up without a slow liveness probe killing it, which is what lets liveness stay aggressive afterwards. |
| `requests` < `limits` | Allows bursting - pod can use up to 500m CPU temporarily without being throttled by default |
| `envFrom.secretRef` | Credentials (DSN, passwords) come from a Kubernetes Secret, not the ConfigMap (which is world-readable within the cluster) |

#### `service.yaml` - stable network address

```yaml
spec:
  selector:
    app: conduit
  ports:
    - port: 80
      targetPort: http  # named port from deployment (8080)
  type: ClusterIP
```

A ClusterIP Service gives all 3 pods a single stable DNS name (`conduit.default.svc.cluster.local`) and load-balances across them. External traffic would go through an Ingress or LoadBalancer Service on top of this.

#### `hpa.yaml` - auto-scaling

```yaml
spec:
  minReplicas: 3
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

When average CPU across all pods exceeds 70%, Kubernetes adds pods (up to 10). When it drops, pods are removed (down to 3). The HPA requires the `metrics-server` addon in the cluster.

#### `configmap.yaml` - environment variables

```yaml
data:
  CONDUIT_WORKER_CONCURRENCY: "50"
  CONDUIT_TRANSPORT: "postgres"
  CONDUIT_LOCK: "advisory"
  ...
```

Non-secret config lives here. Changing a value and re-applying the ConfigMap takes effect on the next pod restart (or immediately if using `envFrom` + a rolling restart).

---

### CI/CD Pipeline

**File:** `.github/workflows/ci.yml`

Four jobs run in order. A later job only starts if the previous one passes.

```
push/PR
  │
  ▼
┌─────────┐     ┌─────────┐     ┌────────────┐     ┌────────────┐
│  test   │────►│  build  │────►│ load-test  │────►│ push-image │
└─────────┘     └─────────┘     └────────────┘     └────────────┘
  vet             compile          docker compose     amd64 + arm64
  unit tests      docker build     real server        GHCR push
  race detector   upload binary    k6 load test       push events only
```

Publishing used to live in a second workflow, `cd.yml`, which ran in parallel with this one.
`needs:` cannot cross workflow files, so a red test suite still shipped an image.
Folding the job in here is what makes the arrow into `push-image` real.

#### `test` job

```yaml
- run: go vet ./...        # static analysis: detects bugs the compiler misses
- run: go test ./...       # unit tests
- run: go test -race ./... # same tests with Go's race detector enabled
```

The **race detector** (`-race`) instruments every memory access at runtime and panics if two goroutines access shared memory without synchronization. It catches data races that unit tests alone miss (e.g., two workers writing to the same map).

#### `build` job (depends on `test`)

```yaml
- run: go build -o bin/conduit ./cmd/server
- run: docker build -t conduit:${{ github.sha }} .
- uses: actions/upload-artifact@v4   # passes binary to next job
```

The binary is uploaded as a GitHub Actions artifact so the `load-test` job can download and run it without recompiling.

#### `load-test` job (depends on `build`)

```yaml
- name: Start infrastructure
  run: docker compose up -d --wait postgres

- name: Apply migrations
  run: bin/conduit migrate

- name: Start server
  run: |
    nohup bin/conduit > /tmp/conduit.log 2>&1 &
    until curl -sf http://localhost:8080/health; do sleep 1; done

- uses: grafana/k6-action@v0.3.1
  with:
    filename: loadtest/k6.js
```

This job spins up the real infrastructure (not mocks), applies the schema with the same `migrate` subcommand a deployment uses, runs the real compiled binary in its default configuration, then hits it with the k6 load test. The k6 script (`loadtest/k6.js`) asserts:
- `p(95) < 50ms`
- Error rate < 1%

#### `push-image` job (depends on `load-test`)

```yaml
if: github.event_name == 'push'     # a pull request never publishes
permissions:
  packages: write                   # only this job gets it
platforms: linux/amd64,linux/arm64
```

`docker/metadata-action` computes the tags, and what they mean is deliberate:

| Trigger | Tags |
|---|---|
| push to `main` | `:main`, `:<sha>` |
| tag `v1.2.3` | `:v1.2.3`, `:v1.2`, `:latest` |

So `:latest` is the last release, not the tip of `main`, which is what someone typing `docker run ghcr.io/justink33/conduit` is asking for.

The `arm64` half needs QEMU plus buildx, and it needs the `Dockerfile` to stop hardcoding `GOARCH`.
`ARG TARGETARCH` is populated by buildx per platform; without it, buildx would happily produce an `arm64` image containing an `amd64` binary that fails on exec rather than at build time.

---

### Interview Questions You Should Know

**Q: What is the difference between a readinessProbe and a livenessProbe?**
A: `readinessProbe` controls whether the pod receives traffic. If it fails, Kubernetes removes the pod from the Service's endpoints - traffic stops going to it, but the pod keeps running. `livenessProbe` controls whether the pod should be restarted. If it fails repeatedly, Kubernetes kills and restarts the pod. You use readiness to handle temporary unavailability (e.g., warming up), and liveness to handle deadlocks or fatal states.

**Q: Why is `terminationGracePeriodSeconds` set to 35 and not 30?**
A: When Kubernetes terminates a pod, it sends SIGTERM and waits `terminationGracePeriodSeconds` before sending SIGKILL. Conduit's graceful shutdown drains the worker pool within 30 seconds (`ShutdownTimeout`). If the grace period were also 30s, there's a race: Kubernetes might SIGKILL the pod at exactly the same moment the drain completes. Setting it to 35s gives the application 5 extra seconds of buffer.

**Q: What is a GitHub Actions artifact and why use one?**
A: An artifact is a file uploaded from one job that later jobs can download. The `build` job compiles the binary and uploads it; `load-test` downloads and runs it. This avoids recompiling in the `load-test` job, saves 20–30 seconds, and ensures the exact same binary that passed compilation is the one being load-tested.

**Q: Why run `go test -race` separately from `go test`?**
A: The race detector adds ~5–10x runtime overhead. Keeping it separate means the basic unit tests still run fast. In CI you want both: fast feedback from the regular tests, and race coverage from the slower instrumented run. The race detector catches an entire class of bugs (data races) that normal tests and even careful code review can miss.

**Q: What is `ClusterIP` and when would you use `LoadBalancer` instead?**
A: `ClusterIP` exposes the Service on a cluster-internal IP - only reachable from within the cluster. Other services in the cluster call `conduit.default.svc.cluster.local`. `LoadBalancer` provisions an external cloud load balancer (e.g., AWS ELB) with a public IP. For Conduit, ClusterIP is correct because the job queue API would sit behind an Ingress controller or API gateway - external traffic never hits the pods directly.

---

## Quick Reference - Files Changed

| Feature | Files |
|---|---|
| Async transport publish | `internal/service/job_service.go` |
| Kafka tuning | `internal/queue/kafka.go`, `pkg/models/models.go`, `internal/config/config.go` |
| pgx tuning | `cmd/server/main.go`, `pkg/models/models.go`, `internal/config/config.go` |
| 3 Redis nodes (behind the `redis` compose profile) | `docker-compose.yml`, `internal/config/config.go`, `.env` |
| Redlock logic | `internal/lock/redlock.go` (unchanged - already correct) |
| Advisory locks, now the default | `internal/lock/postgres.go` |
| K8s manifests | `deploy/k8s/deployment.yaml`, `service.yaml`, `configmap.yaml`, `hpa.yaml` |
| CI/CD | `.github/workflows/ci.yml`, `loadtest/k6.js`, `Dockerfile` |
