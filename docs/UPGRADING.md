# Upgrading

## v0.1.0 to v0.2.0

v0.2.0 changes the wire format. It breaks every v0.1.0 client and every endpoint already receiving Conduit webhooks.
[ADR 0007](decisions/0007-a-strict-wire-contract.md) is why. This page is what to change.

There is no database migration. `task_timeout_ns` is still `BIGINT` and `task_payload` is still `BYTEA`; only the JSON changed.
Existing rows keep working, and `GET /api/jobs/:id` on a job enqueued under v0.1.0 still returns 200.

Read the first section even if you never call the API. It is the only change that breaks in your process rather than at Conduit's door.

### 1. Webhook receivers: `payload` is no longer base64

If anything you run receives Conduit's outbound webhook, this is the change to make first.
`payload` in the request envelope used to be a base64 string. It is now the JSON you enqueued, inline.

Before:

```json
{"job_id":"...","task_name":"webhook","attempt":1,"payload":"eyJvcmRlcl9pZCI6MTIzNH0="}
```

After:

```json
{"job_id":"...","task_name":"webhook","attempt":1,"payload":{"order_id":1234}}
```

So a receiver that did `json.loads(base64.b64decode(body["payload"]))` now does `body["payload"]`.

This one fails on your side rather than on Conduit's, which means Conduit returns a 2xx and your handler raises.
Depending on what your endpoint does with an exception, that can look like a retry storm rather than like a format change.
Update receivers before you deploy the new server.

### 2. `task.timeout` is a duration string

Before: `"timeout": 15000000000`, an integer count of nanoseconds.
After: `"timeout": "15s"`, using [Go's duration syntax](https://pkg.go.dev/time#ParseDuration).

`"90s"`, `"1m30s"`, `"2h"`, and `"500ms"` are all accepted. A bare number is a 400:

```
{"error":{"code":"invalid_request","message":"timeout must be a duration string like \"15s\", got 15000000000"}}
```

The old form is rejected rather than accepted alongside the new one, deliberately.
`15` reads as fifteen seconds to a person and as fifteen nanoseconds to the old format, the two differ by a factor of a billion, and nothing in the value says which was meant.

### 3. `task.payload` is inline JSON

Before: `"payload": "eyJoZWxsbyI6IndvcmxkIn0="`.
After: `"payload": {"hello":"world"}`.

Any valid JSON value works: an object, an array, a string, a number, `null`.
Conduit stores it byte for byte and does not interpret it.
Omit the field entirely rather than sending `""` if there is no payload.

On the way back out, `GET /api/jobs/:id` returns it the same way, so a reader stops decoding too.

### 4. Unknown fields are a 400

A body carrying a field Conduit does not define now returns 400 naming that field, instead of 201 with the field silently discarded.

The common case is `queue` at the top level when it belongs inside `task`:

```jsonc
// rejected: job would have gone to "default" and looked like a success
{"queue":"remote","task":{"name":"webhook"}}

// correct
{"task":{"queue":"remote","name":"webhook"}}
```

This also catches a misspelled field name and a field from a newer version of [WORKERS.md](WORKERS.md) than your server is running.
It applies to every endpoint, including the four pull-protocol ones.

`metadata` is exempt and always will be: its keys are your vocabulary, not Conduit's, so anything goes inside it.

### The one change that does not fail loudly

A v0.1.0 client sending a base64 payload gets a **201**, not a 400.

```
"payload":"aGVsbG8="   ->  201, stored as the string "aGVsbG8="
```

A base64 string is valid JSON, and Conduit does not interpret payloads, so it cannot tell that request apart from a caller who genuinely meant to send that string.
`timeout` and unknown fields both fail at the door. This one does not.

So grep your enqueue call sites for `base64` rather than relying on the API to tell you.
The tell in production is a worker receiving a string where it expected an object.

### Checking you are done

Point a client at a v0.2.0 server and confirm all four:

```bash
# 1. the corrected body is accepted
curl -X POST localhost:8080/api/jobs -H 'content-type: application/json' \
  -d '{"task":{"queue":"default","name":"webhook","metadata":{"url":"https://your-endpoint.example/hook"},"timeout":"15s"}}'
# -> 201 {"id":"..."}

# 2. the old timeout form is refused
curl -X POST localhost:8080/api/jobs -H 'content-type: application/json' \
  -d '{"task":{"name":"webhook","timeout":15000000000}}'
# -> 400, message names the duration string

# 3. a top-level queue is refused
curl -X POST localhost:8080/api/jobs -H 'content-type: application/json' \
  -d '{"queue":"remote","task":{"name":"webhook"}}'
# -> 400, message names "queue"

# 4. payload comes back as JSON, not base64
curl localhost:8080/api/jobs/<id> | jq '.task.payload, .task.timeout'
```

If step 2 or step 3 returns a 201, you are still talking to v0.1.0.
