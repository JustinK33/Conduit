.PHONY: up up-full down restart logs ps test test-race bench vet build tidy migrate enqueue enqueue-elt status list ready worker measure

# Every target below that talks to the API sends this. It is empty by default,
# which is right for a local stack with no keys set; export API_KEY (or put it in
# your shell) once CONDUIT_API_KEYS is set on the server, or every call 401s.
AUTH := $(if $(API_KEY),-H "Authorization: Bearer $(API_KEY)",)
BASE_URL ?= http://localhost:8080

# Start the stack: Postgres, one migration run, and the app. That is all Conduit
# needs.
up:
	docker compose up -d --build

# Add Kafka, Redlock, Prometheus, and Grafana. Neither buys a correctness
# property - see the README's measurements - so this is for reproducing them.
up-full:
	CONDUIT_TRANSPORT=kafka CONDUIT_LOCK=redlock \
		docker compose --profile kafka --profile redis --profile observability up -d --build

# Stop the stack and wipe volumes (fresh state on next `make up`). Every profile,
# so a container started by up-full is not left behind holding the network.
down:
	docker compose --profile kafka --profile redis --profile observability --profile loadtest down -v --remove-orphans

# Full restart: wipe and rebuild from scratch
restart: down up

# Tail logs for all services; use `make logs s=app` to filter
logs:
	docker compose logs -f $(s)

# Show running container status
ps:
	docker compose ps

# Run all unit tests
test:
	go test ./...

# Run tests with the race detector
test-race:
	go test -race ./...

# Run microbenchmarks only (-run=^$$ skips the unit tests)
bench:
	go test -bench=. -benchmem -run=^$$ ./...

# Reproduce every number in the README's "Measured results" section. Needs k6,
# jq, and the loadtest compose profile (it starts a webhook sink). This
# recreates the app container repeatedly, so do not run it against a stack you
# are using for anything else.
measure:
	docker compose --profile kafka --profile redis --profile loadtest up -d --wait
	./loadtest/measure.sh all

# Run the Go static analyzer
vet:
	go vet ./...

# Compile the server binary
build:
	go build -o bin/conduit ./cmd/server

# Download dependencies and tidy go.sum
tidy:
	go mod tidy

# Apply pending migrations against a running Postgres. `make up` already does
# this; run it by hand after adding a migration to an existing stack.
migrate:
	docker compose run --rm migrate

# Enqueue a webhook job - usage: make enqueue url=https://example.com/webhook
enqueue:
	@if [ -z "$(url)" ]; then \
		echo "usage: make enqueue url=https://example.com/webhook"; \
		exit 2; \
	fi
	curl -s -X POST $(BASE_URL)/api/jobs $(AUTH) \
		-H "Content-Type: application/json" \
		-d '{"idempotency_key":"sample-webhook-job","task":{"name":"webhook","timeout":"15s","payload":{"hello":"world"},"metadata":{"url":"$(url)"}}}' | jq .

# Enqueue a SQL ELT job against the optional demo tables from migrations/002_create_elt_demo.sql
# jq builds the request rather than string interpolation: the spec's SQL contains
# a 'paid' literal, and pasting single quotes into a -d '...' argument breaks it.
enqueue-elt:
	jq -c '{idempotency_key:"sample-sql-elt-daily-revenue",task:{name:"sql.etl",max_retries:3,payload:.,metadata:{pipeline:"daily_revenue"}}}' \
		< examples/daily_revenue_pipeline.json \
		| curl -s -X POST $(BASE_URL)/api/jobs $(AUTH) \
			-H "Content-Type: application/json" --data-binary @- | jq .

# Get job status - usage: make status id=<job-id>
status:
	curl -s $(BASE_URL)/api/jobs/$(id) $(AUTH) | jq .

# List jobs - usage: make list, make list state=DEAD
list:
	@if [ -n "$(state)" ]; then \
		curl -s "$(BASE_URL)/api/jobs?state=$(state)&limit=20" $(AUTH) | jq .; \
	else \
		curl -s "$(BASE_URL)/api/jobs?limit=20" $(AUTH) | jq .; \
	fi

# Run the reference worker against the stack - usage: make worker queues=remote
worker:
	QUEUES=$(queues) BASE_URL=$(BASE_URL) API_KEY=$(API_KEY) ./loadtest/worker.sh

# Health probes
ready:
	curl -s $(BASE_URL)/ready | jq .
