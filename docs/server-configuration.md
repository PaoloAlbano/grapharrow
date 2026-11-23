# Server Startup and Authentication

GraphArrow has two server commands. For local development, point both at the
same Native database directory. For replicas on shared Delta storage, see
[Multiple replicas on Kubernetes](#multiple-replicas-on-kubernetes).

| Command | Interfaces | Default address | Authentication |
| --- | --- | --- | --- |
| `grapharrow-server` | REST under `/v1` and ANA at the root | `127.0.0.1:8000` | REST bearer token and a separate ANA client file |
| `grapharrow-bolt` | Bolt 4.4 | `127.0.0.1:7687` | Basic authentication with the configured Bolt token as the password |

The HTTP command includes ANA by default; `--no-ana` disables only ANA. It does
not start Bolt. Both commands initialize the database if needed. Native local
writes are coordinated across the two processes; see
[Transactions and Sessions](transactions.md) for transaction and storage limits.

## Prepare a local configuration

Run these commands from the repository root with Python 3.11 or later and uv:

```sh
uv sync --locked
```

Generate three independent random credentials and a read-only ANA client. This
creates private files under `.local-secrets/`, which is excluded from Git and
Docker build contexts. It does not overwrite an existing configuration.

```sh
uv run --locked python - <<'PY'
import json
import os
import secrets
import shlex
from pathlib import Path

os.umask(0o077)
directory = Path(".local-secrets")
directory.mkdir(mode=0o700, exist_ok=True)
auth_file = directory / "ana-clients.json"
env_file = directory / "server.env"
if auth_file.exists() or env_file.exists():
    raise SystemExit("Configuration already exists; reuse it or choose another directory.")

ana_key = secrets.token_urlsafe(32)
with auth_file.open("x") as stream:
    json.dump({"clients": [{"id": "demo-reader", "api_key": ana_key, "profile": "readonly"}]}, stream)
    stream.write("\n")

settings = {
    "GRAPHARROW_DATABASE_PATH": "/tmp/grapharrow-demo",
    "GRAPHARROW_SERVER_AUTH_TOKEN": secrets.token_urlsafe(32),
    "GRAPHARROW_BOLT_AUTH_TOKEN": secrets.token_urlsafe(32),
    "GRAPHARROW_BOLT_LOG_QUERIES": "false",
    "ANA_AUTH_FILE": str(auth_file.resolve()),
    "ANA_API_KEY": ana_key,
}
with env_file.open("x") as stream:
    for name, value in settings.items():
        stream.write(f"{name}={shlex.quote(value)}\n")
print("Created .local-secrets/server.env and .local-secrets/ana-clients.json")
PY
```

`ANA_API_KEY` is a convenience variable for the client examples, not a server
setting. The server reads ANA credentials from `ANA_AUTH_FILE`. The demo
database uses a temporary directory; choose a persistent location for data you
want to retain. Keep generated credentials out of logs, screenshots and commits.

## Start HTTP: REST and ANA

In the first terminal, load the configuration and start the HTTP process:

```sh
set -a
. ./.local-secrets/server.env
set +a
uv run --locked grapharrow-server --host 127.0.0.1 --port 8000
```

The process stays in the foreground. Keep it running while using another
terminal for clients. It reads the database path from
`GRAPHARROW_DATABASE_PATH`; pass `--path /another/database` to override it.

## Start Bolt

In a second terminal, from the same repository root:

```sh
set -a
. ./.local-secrets/server.env
set +a
uv run --locked grapharrow-bolt --host 127.0.0.1 --port 7687 --no-query-log
```

Both processes now use `/tmp/grapharrow-demo`. Stop each with Ctrl+C. ANA
sessions belong to the HTTP process and end when it shuts down. Use one HTTP
worker; session state is process-local.

## Verify the three interfaces

In a third terminal, load the same configuration:

```sh
set -a
. ./.local-secrets/server.env
set +a
```

Health checks are public and do not require credentials. `/health` is the ANA
liveness endpoint; `/v1/health` also returns database and verification metadata:

```sh
curl --fail-with-body http://127.0.0.1:8000/v1/health
curl --fail-with-body http://127.0.0.1:8000/health
```

REST uses its own bearer token. This query returns the scalar value `1` inside
the REST `data`/`meta` response envelope:

```sh
curl --fail-with-body http://127.0.0.1:8000/v1/query \
  -H "Authorization: Bearer ${GRAPHARROW_SERVER_AUTH_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"query":"RETURN 1 AS value"}'
```

The ANA client first opens a session, then sends both its API key and the
returned session identifier. These commands require curl and the installed
Python environment; they do not require jq:

```sh
ANA_SESSION_ID=$(
  curl --fail-with-body -X POST http://127.0.0.1:8000/sessions \
    -H "Authorization: Bearer ${ANA_API_KEY}" \
  | uv run --locked python -c 'import json, sys; print(json.load(sys.stdin)["session_id"])'
)

curl --fail-with-body http://127.0.0.1:8000/tools/call \
  -H "Authorization: Bearer ${ANA_API_KEY}" \
  -H "X-ANA-Session: ${ANA_SESSION_ID}" \
  -H "Content-Type: application/json" \
  -d '{"tool":"schema_overview","args":{}}'

curl --fail-with-body -X DELETE http://127.0.0.1:8000/sessions/current \
  -H "Authorization: Bearer ${ANA_API_KEY}" \
  -H "X-ANA-Session: ${ANA_SESSION_ID}"
```

See the [ANA interface guide](ana-interface.md) for tool calls, handles and
pagination. Opening a new ANA session captures a new snapshot of the graph.

For Bolt, the development installation includes the Neo4j Python driver used
here. Applications using only the runtime package install their own Bolt client.
The username below is illustrative: GraphArrow validates the basic-auth scheme
and password token; the username does not define a separate role.

```sh
uv run --locked python - <<'PY'
import os
from neo4j import GraphDatabase

with GraphDatabase.driver(
    "bolt://127.0.0.1:7687",
    auth=("grapharrow", os.environ["GRAPHARROW_BOLT_AUTH_TOKEN"]),
) as driver:
    driver.verify_connectivity()
    with driver.session() as session:
        print(session.run("RETURN 1 AS value").single()["value"])
PY
```

Missing or incorrect REST/ANA credentials return HTTP 401. The REST token
cannot authorize an ANA request, and an ANA key cannot authorize a REST request.
Incorrect Bolt credentials cause driver authentication to fail. With this
configuration, REST and Bolt tokens permit writes as well as reads; the ANA
client remains read-only.

## Configuration and network access

| Setting | Purpose |
| --- | --- |
| `GRAPHARROW_DATABASE_PATH` | Database root shared by HTTP and Bolt |
| `GRAPHARROW_STORAGE_READ_ONLY` | Serve an initialized Native database as read-only. Defaults to `false` |
| `GRAPHARROW_NATIVE_WRITER_LOCK_DIR` | Shared local lock directory for HTTP and Bolt processes writing to one Native object-store database |
| `GRAPHARROW_SERVER_HOST` / `GRAPHARROW_SERVER_PORT` | HTTP bind address and port |
| `GRAPHARROW_SERVER_AUTH_TOKEN` | Bearer token for REST query, write and administration routes |
| `GRAPHARROW_REQUEST_METRICS_ENABLED` | Expose HTTP operation and ANA session metrics at `/v1/metrics` in Prometheus text format. Defaults to `false` outside Helm |
| `GRAPHARROW_REQUEST_METRICS_TOKEN` | Optional bearer token scoped to the metrics endpoint |
| `ANA_AUTH_FILE` | JSON file of ANA clients, API keys and profiles |
| `GRAPHARROW_ANA_ENABLED` | Enable ANA in the HTTP process; defaults to `true` |
| `GRAPHARROW_BOLT_HOST` / `GRAPHARROW_BOLT_PORT` | Bolt bind address and port |
| `GRAPHARROW_BOLT_AUTH_TOKEN` | Password token accepted by Bolt basic authentication |
| `GRAPHARROW_BOLT_LOG_QUERIES` | Log Bolt query text; defaults to `true` |

CLI `--path`, `--host` and `--port` options override their environment settings.
Both processes load `GRAPHARROW_*` settings from the environment and can also
read a `.env` file in their working directory. `ANA_AUTH_FILE` is read directly
from the process environment. The explicit `set -a`/`.` commands above export
all settings for the servers and client examples.

Without `GRAPHARROW_SERVER_AUTH_TOKEN`, REST allows unauthenticated queries,
writes and administration. Without `GRAPHARROW_BOLT_AUTH_TOKEN`, Bolt accepts
unauthenticated connections. Without configured ANA clients, protected ANA
routes return HTTP 503. Configuring one interface does not protect the others.

The commands above bind to loopback. `--host 0.0.0.0` listens on all interfaces
but does not enable authentication or encryption. These entrypoints serve
plain HTTP and Bolt; use TLS termination or a secure tunnel for access over
untrusted networks. Keep authentication configured for every exposed interface.

### HTTP and ANA metrics

When enabled, `GET /v1/metrics` uses the metrics token when configured, or
the REST bearer token otherwise. Scrape each HTTP process separately.
The `interface` label identifies `rest_cypher`
(`POST /v1/query`) or `ana` (`POST /tools/call`).

| Metric | Type | Meaning |
| --- | --- | --- |
| `grapharrow_http_operations_in_flight` | Gauge | Requests currently in progress by interface |
| `grapharrow_http_operations_executing` | Gauge | Operations currently inside the Cypher executor or ANA session dispatcher by interface |
| `grapharrow_http_operations_total` | Counter | Finished requests by interface and HTTP status, with `aborted` for requests that end without a response status |
| `grapharrow_http_operation_duration_seconds` | Histogram | Server-side latency from request admission through response handling by interface |
| `grapharrow_http_operation_execution_seconds` | Histogram | Time spent inside the Cypher executor or ANA session dispatcher by interface, including their storage and lock waits |
| `grapharrow_rest_write_conflicts_total` | Counter | Snapshot write conflicts returned by REST |
| `grapharrow_ana_sessions_open` | Gauge | Workflows currently retained by the ANA process, including idle workflows |
| `grapharrow_ana_session_bytes` | Gauge | Estimated bytes charged to the ANA session budget |
| `grapharrow_ana_session_limit_bytes` | Gauge | Configured ANA session budget in bytes |

The duration histograms support fleet-wide percentiles. For example, to
estimate p95 Cypher execution time in seconds across HTTP replicas, run:

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(grapharrow_http_operation_execution_seconds_bucket{interface="rest_cypher"}[5m])
  )
)
```
The [ANA resource limits](ana-interface.md#workflows-and-results) account for
retained state, not a complete CPU or process-memory boundary.

## Arrow IPC over REST

`POST /v1/nodes/bulk` and `POST /v1/edges/bulk` accept Arrow IPC
streams and files. Send `Content-Type: application/vnd.apache.arrow.stream`
for an IPC stream or `application/vnd.apache.arrow.file` for an IPC file.
Both formats use the regular bulk columns and write one graph transaction.
For node ingestion, pass the optional `default_labels` query parameter once
per label, or supply a `labels` column. For relationship ingestion, use
`src_id`, `dst_id` and `rel_type`. Business-key ingestion uses `src_key`
and `dst_key` along with the `source_label`, `source_key`, `target_label`
and `target_key` query parameters.

The bulk endpoints and `POST /v1/query` return IPC when the client sends
the corresponding `Accept` header. Query requests still use a JSON body
with `query` and `params`. JSON remains the default response. The Arrow
schema preserves column names and order. Homogeneous scalar columns and
lists of homogeneous scalars use Arrow types. Maps and heterogeneous
values use a UTF-8 JSON column marked with the field metadata
`grapharrow.encoding=json`. An `Idempotency-Key` replay can select
either representation of the same stored result.

Eligible Cypher scalar projections reuse the engine's Arrow columns.
An IPC stream sends those results as bounded record batches after the
query has prepared its result. The IPC file response includes a complete
file footer and is buffered before sending.

```python
import os

import httpx
import pyarrow as pa

media_type = "application/vnd.apache.arrow.stream"
nodes = pa.table({"name": ["Ada", "Grace"]})
sink = pa.BufferOutputStream()
with pa.ipc.new_stream(sink, nodes.schema) as writer:
    writer.write_table(nodes)

with httpx.Client(
    base_url="http://127.0.0.1:8000",
    headers={"Authorization": f"Bearer {os.environ['GRAPHARROW_SERVER_AUTH_TOKEN']}"},
) as client:
    created = client.post(
        "/v1/nodes/bulk",
        params={"default_labels": "Person"},
        headers={"Content-Type": media_type},
        content=sink.getvalue().to_pybytes(),
    )
    created.raise_for_status()
    response = client.post(
        "/v1/query",
        json={"query": "MATCH (n:Person) RETURN n.name AS name ORDER BY name"},
        headers={"Accept": media_type},
    )
    response.raise_for_status()
    result = pa.ipc.open_stream(response.content).read_all()
```

## Docker

The runtime image uses Python 3.15 on Debian slim. It installs the GraphArrow
wheel and locked runtime dependencies and runs as UID/GID `65532:65532`.

```sh
docker build -t grapharrow:local .
docker volume create grapharrow-demo-data
```

The generated environment file works with `--env-file`. Override the database
and auth-file paths for the container. For this local example, keep
`.local-secrets` at mode 0700 on the host and let the container read the mounted
ANA file. In a shared deployment, mount the credential through the platform's
secret system with read access for GID 65532.

```sh
chmod 0700 .local-secrets
chmod 0644 .local-secrets/ana-clients.json
```

In the first terminal:

```sh
docker run --rm --name grapharrow-http \
  --read-only --tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --env-file .local-secrets/server.env \
  -e GRAPHARROW_DATABASE_PATH=/data/grapharrow \
  -e ANA_AUTH_FILE=/run/grapharrow/ana-clients.json \
  --mount "type=bind,source=$(pwd)/.local-secrets/ana-clients.json,target=/run/grapharrow/ana-clients.json,readonly" \
  --mount type=volume,source=grapharrow-demo-data,target=/data \
  -p 127.0.0.1:8000:8000 \
  grapharrow:local
```

In a second terminal, run Bolt from the same image and share the same volume:

```sh
docker run --rm --name grapharrow-bolt \
  --read-only --tmpfs /tmp:rw,nosuid,nodev,noexec,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --env-file .local-secrets/server.env \
  -e GRAPHARROW_DATABASE_PATH=/data/grapharrow \
  --mount type=volume,source=grapharrow-demo-data,target=/data \
  -p 127.0.0.1:7687:7687 \
  --entrypoint /app/.venv/bin/grapharrow-bolt \
  grapharrow:local --host 0.0.0.0 --port 7687 --no-query-log
```

Run either the host processes or these containers when using the same host
ports. The image's default command starts HTTP/ANA only; Bolt uses the explicit
entrypoint above. Container listeners bind to all container interfaces, while
the `-p 127.0.0.1:...` mappings expose them only on the host loopback address.
The named volume retains database files after containers stop; removing an ANA
session or stopping a server does not delete that data.

A new named volume inherits the image's `/data` ownership. For existing
volumes and bind mounts, arrange read and write access for UID/GID 65532
before updating. Check the entire
database tree, including files last written by a root-run container.

## Kubernetes with Helm

The [deployment guide](kubernetes.md) covers a single Native replica on a PVC
and multiple Delta replicas on shared S3 or Azure Blob Storage.

## Multiple replicas on Kubernetes

The [two-replica example](../examples/kubernetes/ana-affinity/) runs GraphArrow
against one Delta database on shared object storage. One gateway Service exposes
REST and ANA on port 8000 and Bolt on port 7687. Each GraphArrow Pod runs one HTTP
process and one Bolt process.

Set `GRAPHARROW_ANA_SESSION_ROUTE` to a unique DNS label per HTTP process
(lowercase letters, digits and internal hyphens, up to 63 characters). When it
is set, both `session_id` and `X-ANA-Session` contain
`<route>.<random-token>`. Standalone servers can leave the setting unset.

The example uses each StatefulSet Pod name as the route and gives that Pod
a [dedicated Service](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#pod-name-label).
Both HAProxy replicas use the same routing rules. They balance session
creation, discovery and REST traffic across healthy HTTP replicas. They send
`/tools/call` and `/sessions/current` to the replica named by the
`X-ANA-Session` prefix. Bolt connections are balanced separately at the TCP
layer. The route only selects a destination. GraphArrow validates the full
session token and its authenticated owner.

Before applying the example, point both replicas at an initialized Delta
database on [shared storage](storage-backends.md#delta-lake-setup). Put
`GRAPHARROW_DATABASE_PATH` and any provider settings the containers need in
`.local-secrets/storage.env`, as unquoted `NAME=value` entries. Build
`grapharrow:local` with the [Docker instructions](#docker). For kind, load it
with `kind load docker-image grapharrow:local --name CLUSTER`. For a remote
cluster, set both container images in the StatefulSet to a GraphArrow build
accessible to that cluster. Use the credentials from
[Prepare a local configuration](#prepare-a-local-configuration).

```sh
set -a
. ./.local-secrets/server.env
set +a

kubectl create namespace grapharrow-demo
kubectl -n grapharrow-demo create secret generic grapharrow-storage \
  --from-env-file=.local-secrets/storage.env
kubectl -n grapharrow-demo create secret generic grapharrow-auth \
  --from-literal=rest-token="$GRAPHARROW_SERVER_AUTH_TOKEN" \
  --from-literal=bolt-token="$GRAPHARROW_BOLT_AUTH_TOKEN" \
  --from-file=clients.json=.local-secrets/ana-clients.json
kubectl -n grapharrow-demo apply -k examples/kubernetes/ana-affinity
kubectl -n grapharrow-demo rollout status statefulset/grapharrow
kubectl -n grapharrow-demo rollout status deployment/grapharrow-gateway
kubectl -n grapharrow-demo port-forward service/grapharrow-gateway 8000:8000 7687:7687
```

Use the [HTTP examples](#start-http-rest-and-ana) and
[Bolt client instructions](#start-bolt) through the forwarded ports. For
external traffic, terminate TLS at the deployment boundary.

Handles live in the creating HTTP process. If that process restarts, clients
open a new workflow and recreate its handles. A temporarily unavailable
replica may return a gateway error for its workflows. Add a per-Pod Service
and the corresponding HAProxy routes and pool entries when adding a replica.
