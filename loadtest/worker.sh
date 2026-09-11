#!/usr/bin/env bash
#
# A complete Conduit worker, in shell. This is the reference implementation of
# the pull protocol: if it needed a client library to be usable, the protocol
# would be the wrong shape.
#
#   loadtest/worker.sh                       # claim from any queue
#   QUEUES=remote,gpu loadtest/worker.sh     # claim from named queues
#   API_KEY=... loadtest/worker.sh           # when API_KEYS is set on the server
#
# It prints what it does and exits on Ctrl-C. Requires curl and jq.
#
# What it deliberately does not do: heartbeat. A worker whose jobs run longer
# than the server's lease must POST /api/jobs/:id/heartbeat every lease/2 or the
# reconciler will requeue the job underneath it. See docs/WORKERS.md.
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
QUEUES="${QUEUES:-}"
API_KEY="${API_KEY:-}"
IDLE_SLEEP="${IDLE_SLEEP:-2}"

auth=()
if [ -n "$API_KEY" ]; then
  auth=(-H "Authorization: Bearer $API_KEY")
fi

# api METHOD PATH [BODY] -> body, then a final line holding the status code.
# One place speaks HTTP so the header and the status trick are not repeated.
# ${auth[@]+...} is for bash 3.2, which treats an empty array as unset under -u.
api() {
  curl -sS -X "$1" "$BASE_URL$2" \
    -H 'Content-Type: application/json' \
    ${auth[@]+"${auth[@]}"} \
    -d "${3:-}" -w '\n%{http_code}'
}

# An empty queue list claims from every queue, which will win jobs this worker
# has no code for. Name them in anything real.
if [ -n "$QUEUES" ]; then
  claim_body=$(jq -cn --arg q "$QUEUES" '{queues: ($q | split(","))}')
else
  claim_body='{}'
fi

echo "worker: polling $BASE_URL, queues=${QUEUES:-<all>}"

while true; do
  response=$(api POST /api/jobs/claim "$claim_body")
  status="${response##*$'\n'}"
  body="${response%$'\n'*}"

  if [ "$status" = "204" ]; then
    # Nothing was due. There is no long-poll yet, so polling is the protocol.
    sleep "$IDLE_SLEEP"
    continue
  fi
  if [ "$status" != "200" ]; then
    echo "worker: claim failed with $status: $body" >&2
    sleep "$IDLE_SLEEP"
    continue
  fi

  job_id=$(jq -r '.job.id' <<<"$body")
  task_name=$(jq -r '.job.task.name' <<<"$body")
  lease_token=$(jq -r '.lease_token' <<<"$body")
  # Payload is base64 on the wire because Task.Payload is a Go []byte.
  payload=$(jq -r '.job.task.payload // "" | @base64d' <<<"$body")

  echo "worker: claimed $job_id ($task_name)"

  # ---- the actual work goes here -------------------------------------------
  # Succeeds unless the payload mentions "fail", which is enough to exercise
  # both report paths from a shell.
  if [[ "$payload" == *fail* ]]; then
    work_ok=false
  else
    work_ok=true
  fi
  # -------------------------------------------------------------------------

  if [ "$work_ok" = true ]; then
    # The lease token proves this worker still owns the job. If the lease
    # expired and the reconciler requeued it, this returns 409 lease_lost, and
    # the correct response is to drop the result rather than retry the report:
    # something else is already running the job.
    out=$(api POST "/api/jobs/$job_id/complete" \
      "$(jq -cn --arg t "$lease_token" '{lease_token: $t, metadata: {worker: "worker.sh"}}')")
  else
    # retry:true asks the server to apply its backoff policy, which may still
    # dead-letter the job when the attempt budget is spent. retry:false means
    # never retry, whatever the policy says.
    out=$(api POST "/api/jobs/$job_id/fail" \
      "$(jq -cn --arg t "$lease_token" '{lease_token: $t, error: "payload asked to fail", retry: true}')")
  fi

  echo "worker: reported $job_id -> ${out##*$'\n'} ${out%$'\n'*}"
done
