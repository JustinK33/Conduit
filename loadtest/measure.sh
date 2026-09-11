#!/usr/bin/env bash
#
# Produces every number in the README's "Measured results" section.
#
#   make measure                  # everything
#   loadtest/measure.sh drain     # one section
#
# Requires docker compose, k6, jq, python3, and the loadtest compose profile
# (it brings up a webhook sink). It recreates the `app` container between runs
# to change its configuration, so it will disrupt anything else on this stack.
#
# Host ports 5433 (Postgres) and 3000 (Grafana) are often taken by other
# projects; override with POSTGRES_HOST_PORT / GRAFANA_HOST_PORT.
set -euo pipefail

cd "$(dirname "$0")/.."

POSTGRES_HOST_PORT="${POSTGRES_HOST_PORT:-5433}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
SINK_URL="${SINK_URL:-http://sink:8080}"
WORKERS="${WORKERS:-8}"
BATCH="${BATCH:-2000}"
OUT_DIR="${OUT_DIR:-/tmp/conduit-measure}"

export POSTGRES_HOST_PORT
mkdir -p "$OUT_DIR"

psql_q() { docker compose exec -T postgres psql -U conduit -d conduit -tAc "$1"; }
now_s() { python3 -c 'import time; print(time.time())'; }
since() { python3 -c "import time; print(round(time.time()-$1, 2))"; }

# recreate_app restarts app with the given VAR=VALUE overrides. docker-compose.yml
# interpolates these into the app service's environment.
recreate_app() {
  env "$@" POSTGRES_HOST_PORT="$POSTGRES_HOST_PORT" \
    docker compose up -d --force-recreate --wait app >/dev/null
  for _ in $(seq 1 60); do
    [ "$(curl -fsS "$BASE_URL/ready" 2>/dev/null | jq -r .status)" = "ready" ] && return 0
    sleep 1
  done
  echo "app did not become ready" >&2
  docker compose logs app --tail 30 >&2
  return 1
}

reset_jobs() { psql_q "TRUNCATE jobs;" >/dev/null; }
states() { psql_q "SELECT state || '=' || count(*) FROM jobs GROUP BY state ORDER BY state;" | paste -sd, -; }

# sample_pending appends one PENDING count per line until the marker file is removed.
sample_pending() {
  local marker="$1" out="$2"
  : >"$out"
  while [ -f "$marker" ]; do
    psql_q "SELECT count(*) FROM jobs WHERE state = 'PENDING';" >>"$out" 2>/dev/null || true
    sleep 0.25
  done
}

# enqueue_batch posts $1 webhook jobs at $2-way parallelism against the sink path $3.
enqueue_batch() {
  local n="$1" par="${2:-20}" path="${3:-/status/200}"
  seq 1 "$n" | xargs -P "$par" -I{} \
    curl -fsS -o /dev/null -X POST "$BASE_URL/api/jobs" \
      -H 'Content-Type: application/json' \
      -d "{\"task\":{\"name\":\"webhook\",\"max_retries\":3,\"metadata\":{\"url\":\"$SINK_URL$path\"}}}"
}

# ---------------------------------------------------------------------------
# intake: how fast the API can accept jobs. Uses TASK=k6-load-test, which has
# no registered handler, so this deliberately measures intake only. Those jobs
# fail on execution, which trips the global circuit breaker; see the README.
# ---------------------------------------------------------------------------
run_intake() {
  local label="intake"
  echo "### $label (k6 100 VUs, task=k6-load-test, workers=$WORKERS)" >&2
  reset_jobs

  local marker="$OUT_DIR/$label.running" depth="$OUT_DIR/$label.depth"
  touch "$marker"; sample_pending "$marker" "$depth" & local sampler=$!

  local summary="$OUT_DIR/$label.json"
  env TASK=k6-load-test BASE_URL="$BASE_URL" \
    k6 run --quiet --summary-export "$summary" loadtest/k6.js >"$OUT_DIR/$label.k6.txt" 2>&1 || true

  rm -f "$marker"; wait "$sampler" 2>/dev/null || true

  printf 'intake|req_per_s=%s|api_p50=%s|api_p95=%s|api_p99=%s|fail_rate=%s|accepted=%s|depth_max=%s\n' \
    "$(jq -r '.metrics.http_reqs.rate|floor' "$summary")" \
    "$(jq -r '.metrics.http_req_duration["p(50)"]|.*100|round/100' "$summary")" \
    "$(jq -r '.metrics.http_req_duration["p(95)"]|.*100|round/100' "$summary")" \
    "$(jq -r '.metrics.http_req_duration["p(99)"]|.*100|round/100' "$summary")" \
    "$(jq -r '.metrics.http_req_failed.value' "$summary")" \
    "$(psql_q 'SELECT count(*) FROM jobs;')" \
    "$(sort -n "$depth" | tail -1)" | tee -a "$OUT_DIR/results.txt"
}

# ---------------------------------------------------------------------------
# drain: the honest execution number. Enqueue a bounded batch of real webhook
# jobs, then time how long the system takes to put all of them in a terminal
# state. Rate is jobs/s over the whole window, latency is enqueue-to-terminal.
# ---------------------------------------------------------------------------
run_drain() {
  local label="$1"; shift
  echo "### drain $label ($BATCH webhook jobs, workers=$WORKERS)" >&2
  reset_jobs

  local t0; t0=$(now_s)
  enqueue_batch "$BATCH"
  local enqueue_s; enqueue_s=$(since "$t0")

  local drained=""
  for _ in $(seq 1 1200); do
    if [ "$(psql_q "SELECT count(*) FROM jobs WHERE state IN ('COMPLETED','DEAD');")" = "$BATCH" ]; then
      drained=$(since "$t0"); break
    fi
    sleep 0.25
  done

  # percentile_disc over enqueue-to-terminal wall clock, in milliseconds.
  local lat
  lat=$(psql_q "
    SELECT round(percentile_disc(0.50) WITHIN GROUP (ORDER BY ms)::numeric,1)
      || '|p95=' || round(percentile_disc(0.95) WITHIN GROUP (ORDER BY ms)::numeric,1)
      || '|p99=' || round(percentile_disc(0.99) WITHIN GROUP (ORDER BY ms)::numeric,1)
      || '|max=' || round(max(ms)::numeric,1)
    FROM (SELECT extract(epoch FROM (updated_at - created_at))*1000 AS ms
          FROM jobs WHERE state IN ('COMPLETED','DEAD')) t;")

  local rate="n/a"
  [ -n "$drained" ] && rate=$(python3 -c "print(round($BATCH/$drained, 1))")

  printf 'drain-%s|n=%s|enqueue_s=%s|drain_s=%s|jobs_per_s=%s|e2e_p50=%s|%s\n' \
    "$label" "$BATCH" "$enqueue_s" "${drained:-timeout}" "$rate" "$lat" "$(states)" \
    | tee -a "$OUT_DIR/results.txt"
}

# ---------------------------------------------------------------------------
# dispatch: what Kafka actually buys. Enqueue jobs one at a time with a gap so
# each is dispatched from an otherwise idle queue, then read the latency
# distribution out of Postgres in one shot. Run with the broker up and killed.
# ---------------------------------------------------------------------------
run_dispatch_latency() {
  local label="$1" n="${2:-20}"
  echo "### dispatch $label ($n jobs, 1s apart, idle queue)" >&2
  reset_jobs
  for _ in $(seq 1 "$n"); do
    enqueue_batch 1 1
    sleep 1
  done
  sleep 20

  local lat
  lat=$(psql_q "
    SELECT count(*)
      || '|p50=' || round(percentile_disc(0.50) WITHIN GROUP (ORDER BY ms)::numeric,1)
      || '|p95=' || round(percentile_disc(0.95) WITHIN GROUP (ORDER BY ms)::numeric,1)
      || '|max=' || round(max(ms)::numeric,1)
    FROM (SELECT extract(epoch FROM (updated_at - created_at))*1000 AS ms
          FROM jobs WHERE state IN ('COMPLETED','DEAD')) t;")
  printf 'dispatch-%s|n=%s|%s\n' "$label" "$n" "$lat" | tee -a "$OUT_DIR/results.txt"
}

# ---------------------------------------------------------------------------
# crash: SIGKILL the app while jobs are RUNNING, then time recovery. Recovery is
# bounded below by CONDUIT_RECONCILER_RUNNING_LEASE, which is the whole point.
# ---------------------------------------------------------------------------
run_crash() {
  local lease="${1:-15s}" jobs="${2:-20}"
  echo "### crash (lease=$lease, jobs=$jobs)" >&2
  WORKERS="$jobs" base_app CONDUIT_RECONCILER_RUNNING_LEASE="$lease"
  reset_jobs

  # /delay/8 holds each job in RUNNING long enough to be killed mid-execution.
  enqueue_batch "$jobs" "$jobs" /delay/8

  for _ in $(seq 1 40); do
    [ "$(psql_q "SELECT count(*) FROM jobs WHERE state='RUNNING';")" -ge 1 ] && break
    sleep 0.25
  done

  local running_at_kill t0
  running_at_kill=$(psql_q "SELECT count(*) FROM jobs WHERE state='RUNNING';")
  t0=$(now_s)
  docker compose kill -s KILL app >/dev/null 2>&1
  docker compose start app >/dev/null 2>&1 || true

  local t_requeue="" t_terminal=""
  for _ in $(seq 1 600); do
    local stuck done_n
    stuck=$(psql_q "SELECT count(*) FROM jobs WHERE state='RUNNING';" 2>/dev/null || echo "$running_at_kill")
    [ -z "$t_requeue" ] && [ "$stuck" = "0" ] && t_requeue=$(since "$t0")
    done_n=$(psql_q "SELECT count(*) FROM jobs WHERE state IN ('COMPLETED','DEAD');" 2>/dev/null || echo 0)
    if [ "$done_n" = "$jobs" ]; then t_terminal=$(since "$t0"); break; fi
    sleep 0.25
  done

  printf 'crash|lease=%s|running_at_kill=%s|requeue_s=%s|all_terminal_s=%s|%s\n' \
    "$lease" "$running_at_kill" "${t_requeue:-timeout}" "${t_terminal:-timeout}" "$(states)" \
    | tee -a "$OUT_DIR/results.txt"
}

# base_app starts app with a fresh Kafka topic and consumer group. Without this,
# a run that overflows the worker pool leaves thousands of uncommitted messages
# behind, and the next run's consumer spends minutes replaying jobs that no
# longer exist while the reconciler does all the real dispatching.
#
# The topic has to exist *before* the app starts. Sarama's consumer group only
# learns about topics at join time and on Metadata.RefreshFrequency (10 minutes
# by default), so a group that joins on an auto-created-later topic is assigned
# no partitions and consumes nothing for the whole run.
base_app() {
  local run_id topic
  run_id="m$(date +%s)"
  topic="conduit-jobs-$run_id"
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$topic" --partitions 3 >/dev/null 2>&1
  recreate_app \
    CONDUIT_WEBHOOK_ALLOW_PRIVATE_NETWORKS=true \
    CONDUIT_WORKER_CONCURRENCY="$WORKERS" \
    CONDUIT_KAFKA_TOPIC="$topic" \
    CONDUIT_KAFKA_CONSUMER_GROUP="conduit-workers-$run_id" \
    "$@"
}

case "${1:-all}" in
  intake)   base_app; run_intake ;;
  drain)    base_app; run_drain "defaults" ;;
  drain-b)  base_app CONDUIT_REDIS_ADDRESSES=redis:6379; run_drain "redis1" ;;
  # The defaults throttle dispatch, not execution: a burst bigger than
  # CONDUIT_WORKER_QUEUE_SIZE spills off the Kafka fast path onto the reconciler,
  # capped at CONDUIT_RECONCILER_BATCH_SIZE per tick. Same run, untuned.
  drain-tuned)
    WORKERS=32 base_app CONDUIT_RECONCILER_BATCH_SIZE=2000 CONDUIT_WORKER_QUEUE_SIZE=4096
    WORKERS=32 run_drain "tuned"
    ;;
  dispatch) base_app; run_dispatch_latency "kafka-up" "${2:-20}" ;;
  dispatch-nokafka)
    base_app
    echo "### killing kafka: the reconciler becomes the only dispatch path" >&2
    docker compose kill kafka >/dev/null 2>&1
    run_dispatch_latency "kafka-down" "${2:-20}"
    docker compose up -d --wait kafka >/dev/null
    ;;
  crash)    run_crash "${2:-15s}" "${3:-20}" ;;
  all)
    : >"$OUT_DIR/results.txt"
    "$0" intake
    "$0" drain
    "$0" drain-tuned
    "$0" drain-b
    "$0" dispatch
    "$0" dispatch-nokafka
    "$0" crash
    echo; echo "== $OUT_DIR/results.txt =="; cat "$OUT_DIR/results.txt"
    ;;
  *) echo "usage: $0 [all|intake|drain|drain-tuned|drain-b|dispatch|dispatch-nokafka|crash]" >&2; exit 2 ;;
esac
