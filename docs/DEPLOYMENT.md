# Deploying Conduit

Conduit does not speak TLS, and four of its endpoints are deliberately unauthenticated.
Both of those are the right decisions, and both of them mean the same thing: **there is a reverse proxy in a correct deployment, and it is not optional.**

Serving Conduit directly on an untrusted network is unsupported.
This page is the recipe.

## Why a proxy, specifically

TLS is the obvious half.
`API_KEYS` are bearer tokens, and a bearer token over plain HTTP is a token in cleartext, readable by anything on the path.
Building TLS into the process would mean owning certificates, ACME, renewal, SNI, and cipher policy, which Caddy and nginx already own and do better.

The less obvious half is path gating, and it matters more than it looks.
`APIKeyAuth` mounts on the `/api/jobs` group only, so that Kubernetes probes, `ci.yml`'s readiness poll, and the Prometheus scrape keep working without a credential.
That is correct inside a private network and dangerous the moment the port is published wholesale:

| Path | Auth | Who should reach it |
| --- | --- | --- |
| `/api/jobs/*` | `API_KEYS`, when set | Clients and workers, over TLS |
| `/live` | None | Anyone; it touches no dependency and reveals only that the process is up |
| `/ready` | None | Private only. It names the liveness of Postgres and every Redis node. |
| `/health` | None | Private only. Alias of `/live`, kept for compatibility. |
| `/metrics` | None | Private only. Queue depth, throughput, failure counts. |

So the proxy is what makes the unauthenticated endpoints private, which is what lets them stay unauthenticated.

It also covers three things Conduit has no code for and should not grow code for:

- **Rate limiting.** `POST /api/jobs/claim` is a cheap call that costs a database query, so a flood is a load amplifier against Postgres. A leaked API key is a denial-of-service primitive even with auth working perfectly.
- **A request body cap.** `Task.Payload` is a `[]byte` with no bound anywhere in Conduit.
- **Connection limits.**

## Bind to loopback

`HTTP_ADDRESS` defaults to `:8080`, every interface.
On a VM where the proxy is a local process, set it so nothing can bypass the proxy by hitting the port directly:

```
HTTP_ADDRESS=127.0.0.1:8080
```

In Docker the equivalent is not publishing the port at all: drop the `ports:` mapping for the `app` service and put the proxy on the same compose network, reaching Conduit as `app:8080`.
In Kubernetes the Service is already cluster-internal; keep it `ClusterIP` and put an Ingress in front.

## TRUSTED_PROXIES

gin trusts every proxy by default, and `internal/api/middleware.go` logs `client_ip` on every request.
Untrusted, that means any client can forge its own address in your audit log with an `X-Forwarded-For` header.

`TRUSTED_PROXIES` is a comma-separated CIDR list, and empty is the safe default: it makes gin ignore forwarding headers entirely and use the direct peer address.

Set it only to the network the proxy actually sits on, and only once something is genuinely in front:

```
TRUSTED_PROXIES=127.0.0.1/32          # local Caddy or nginx
TRUSTED_PROXIES=10.0.0.0/8            # a pod or VPC network
```

Verify it both ways rather than assuming.
Unset, a request carrying a forged `X-Forwarded-For` must not change the logged `client_ip`.
Set, the logged `client_ip` must be the real client rather than the proxy.

## Caddy

The default recommendation, because certificate provisioning and renewal are automatic and the config is short enough to audit.

```caddyfile
queue.example.com {
	# The API. Authenticated by Conduit via API_KEYS.
	handle /api/* {
		request_body {
			max_size 1MB
		}
		reverse_proxy 127.0.0.1:8080
	}

	# Liveness only, for an external uptime check. No auth, but it reveals
	# nothing: it is a process-is-up check that touches no dependency.
	handle /live {
		reverse_proxy 127.0.0.1:8080
	}

	# Everything else is private, and that is deliberate rather than an
	# oversight. /metrics is queue depth and failure counts. /ready and
	# /health name the state of Postgres and every Redis node. All three
	# are unauthenticated inside Conduit so that probes and Prometheus
	# work, which means this block is what keeps them off the internet.
	handle {
		respond 404
	}
}
```

Caddy has no built-in rate limiter, so that needs the third-party `caddy-ratelimit` module.

## nginx

Where rate limiting is non-negotiable, nginx has it in the box and is the better pick.

```nginx
# Key the limit on the API key rather than the source address, so one
# noisy worker fleet behind NAT cannot exhaust everyone else's budget.
limit_req_zone $http_authorization zone=conduit_api:10m rate=50r/s;

server {
    listen 443 ssl;
    http2 on;
    server_name queue.example.com;

    ssl_certificate     /etc/letsencrypt/live/queue.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/queue.example.com/privkey.pem;

    client_max_body_size 1m;

    location /api/ {
        limit_req zone=conduit_api burst=100 nodelay;
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host              $host;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Must exceed the long-poll wait when phase 5 lands, or claims get
        # cut off mid-wait and workers see spurious errors.
        proxy_read_timeout 60s;
        proxy_send_timeout 60s;
    }

    location = /live {
        proxy_pass http://127.0.0.1:8080;
    }

    # /metrics, /ready, /health stay private. See the Caddy notes above.
    location / {
        return 404;
    }
}
```

With nginx in front, set `TRUSTED_PROXIES=127.0.0.1/32` so `X-Forwarded-For` is honoured from it and from nothing else.

## Kubernetes

`deploy/k8s/` has the Deployment, Service, ConfigMap, HPA, and a Secret template.

The same shape applies: an Ingress plus cert-manager, with the Ingress routing `/api` and `/live` only, and the Service left `ClusterIP` so nothing else is externally reachable.

`API_KEYS` belongs in the Secret, never the ConfigMap.
Copy `deploy/k8s/secret.example.yaml`, generate a real key, and do not commit the result.

```
kubectl create secret generic conduit-secrets \
  --from-literal=API_KEYS="$(openssl rand -hex 32)" \
  --from-literal=POSTGRES_DSN='postgres://...'
```

Prometheus scrapes the pod or Service directly on the cluster network, never through the public proxy, so `deploy/prometheus/prometheus.yml` needs no change.

## Generating and rotating keys

```
openssl rand -hex 32
```

Each key must be at least 16 characters or the server refuses to start.
`API_KEYS` accepts a comma-separated list and any listed key is accepted, so rotation is: add the new key, restart, move workers across, remove the old one.

Keys go in `.env` or a Secret. Never in a config file that is committed, and never in a ConfigMap.

## Checklist

- [ ] `API_KEYS` set to at least one 32-byte random key. Absent means the API is open, and the server warns about it at boot.
- [ ] TLS terminated by something in front. The server warns unconditionally that keys transit in clear, because it cannot see what is upstream.
- [ ] `/metrics`, `/ready`, and `/health` return 404 through the proxy while still returning 200 on the private address. Test it, do not assume it.
- [ ] `HTTP_ADDRESS` on loopback, or the port unpublished.
- [ ] `TRUSTED_PROXIES` matching the proxy's network, or empty.
- [ ] `WORKER_QUEUES` set if remote workers exist, or the server competes for their jobs and dead-letters them. See [WORKERS.md](WORKERS.md).
- [ ] `POSTGRES_DSN` pointing at a database with backups. Postgres is the source of truth; losing it loses the queue.

## Known gaps

- **Migrations only run on a fresh volume.** The schema is applied by mounting `migrations/` into the Postgres entrypoint, which runs once on an empty data directory. A schema change does not reach an existing database. A `migrate` subcommand is [phase 3](ROADMAP.md#phase-3---kafka-and-redis-become-optional).
- **`METRICS_LISTEN_ADDRESS` is parsed and ignored.** `/metrics` is served on the main HTTP port, which is why the proxy has to gate it. A separate metrics listener would be the better answer.
- **The API key is all-or-nothing.** Holding it means claiming, cancelling, and reading every job. Per-key identity is [phase 2](ROADMAP.md#phase-2---more-than-one-tenant).
- **`deploy/k8s/deployment.yaml` names `conduit:latest`**, which resolves to nothing. Point it at your registry until [phase 6](ROADMAP.md#phase-6---something-an-adopter-can-actually-pin) lands published images.
