# Server Startup and Authentication

GraphArrow has two server commands. Run both against the same native local
database directory when clients need HTTP and Bolt access to one graph.

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
| `GRAPHARROW_SERVER_HOST` / `GRAPHARROW_SERVER_PORT` | HTTP bind address and port |
| `GRAPHARROW_SERVER_AUTH_TOKEN` | Bearer token for REST query, write and administration routes |
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
The [ANA resource limits](ana-interface.md#workflows-and-results) account for
retained state, not a complete CPU or process-memory boundary.

## Docker

Build the runtime image once:

```sh
docker build -t grapharrow:local .
docker volume create grapharrow-demo-data
```

The generated environment file works with `--env-file`. Override the database
and auth-file paths for the container. In the first terminal:

```sh
docker run --rm --name grapharrow-http \
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
