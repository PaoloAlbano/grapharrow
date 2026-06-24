<div align="center">

  <img src="images/grapharrow_1.jpg" alt="Pipol" width="128" />

# GraphArrow
</div>

GraphArrow is an embedded property graph database built in Python on top of PyArrow and Parquet.

The project is currently in the bootstrap phase. The repository already contains the main architecture decisions, and this skeleton provides the initial package layout, tooling configuration, and test scaffolding to start implementation.

## Goals

- Embedded Python-first graph database
- Columnar storage with PyArrow
- Parquet-backed persistence
- Cypher / openCypher query support
- Local and remote object storage support
- Snapshot-based storage and transaction model

## Getting Started

This project uses `uv` as the package manager.

### Install dependencies

```bash
uv sync
# or
make sync-dev
```

### Run tests

```bash
uv run pytest
# or
make test
```

### Run lint checks

```bash
uv run ruff check .
# or
make lint
```

### Run type checks

```bash
uv run mypy src
# or
make typecheck
```

### Run the full local check suite

```bash
make check
```

### Run the backend comparison benchmark

```bash
make benchmark-backends
# or
uv run python scripts/benchmark_backends.py
```

The benchmark writes:

- `docs/benchmarks/data-access-backend-benchmark.md`
- `docs/benchmarks/data-access-backend-benchmark.json`

It compares `pyarrow`, `pandas`, `polars`, and `duckdb` on a shared synthetic Parquet dataset.

### Explore the CLI

```bash
uv run grapharrow --help
# or
make cli-help
```

### Explore the REST server

```bash
uv run grapharrow-server --help
# or
make server-help
```

### Explore the Bolt bootstrap entrypoint

```bash
uv run grapharrow-bolt --help
# or
make bolt-help
```

### Run the REST server

```bash
uv run grapharrow-server --path /tmp/grapharrow-db
# or
make serve DB=/tmp/grapharrow-db
```

The server can also read the database path from `GRAPHARROW_DATABASE_PATH`.

If `GRAPHARROW_SERVER_AUTH_TOKEN` is configured, every REST endpoint except `/v1/health`
requires `Authorization: Bearer <token>`.

`grapharrow-bolt` is currently a wiring/bootstrap entrypoint only. It already resolves settings
and database paths, and now exposes a minimal Bolt 4.4 server with basic auth,
`RUN`, `PULL`, `DISCARD`, `RESET`, and `GOODBYE`.

### REST endpoints

Current bootstrap endpoints:

- `GET /v1/health`
- `POST /v1/query`
- `POST /v1/nodes`
- `POST /v1/edges`
- `POST /v1/triples`
- `GET /v1/admin/verify`
- `POST /v1/admin/recover`

### Example REST calls

Create a node with the imperative API:

```bash
curl -X POST http://127.0.0.1:8000/v1/nodes \
  -H "Content-Type: application/json" \
  -d '{
    "labels": ["Person"],
    "properties": {
      "external_id": "alice",
      "name": "Alice"
    }
  }'
```

Run a Cypher query:

```bash
curl -X POST http://127.0.0.1:8000/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (n:Person) WHERE n.name = $name RETURN n",
    "params": {
      "name": "Alice"
    }
  }'
```

Create an edge using explicit business-key references:

```bash
curl -X POST http://127.0.0.1:8000/v1/edges \
  -H "Content-Type: application/json" \
  -d '{
    "source_ref": {
      "label": "Person",
      "key": "external_id",
      "value": "alice"
    },
    "target_ref": {
      "label": "Person",
      "key": "external_id",
      "value": "bob"
    },
    "rel_type": "KNOWS",
    "properties": {
      "since": 2025
    }
  }'
```

### Bootstrap and inspect a local database

```bash
uv run grapharrow init /tmp/grapharrow-db
uv run grapharrow show-catalog /tmp/grapharrow-db
uv run grapharrow show-snapshot /tmp/grapharrow-db
uv run grapharrow verify /tmp/grapharrow-db
uv run grapharrow recover /tmp/grapharrow-db
```

The same workflows are also exposed through `make` targets that accept `DB=/path/to/database`.

### Bulk ingest from the CLI

Import nodes from a JSON array:

```bash
uv run grapharrow bulk-add-nodes /tmp/grapharrow-db ./nodes.json --default-label Person
```

Import edges from a JSONL file using explicit business-key references:

```bash
uv run grapharrow bulk-add-edges /tmp/grapharrow-db ./edges.jsonl \
  --source-label Person \
  --source-key external_id \
  --target-label Person \
  --target-key external_id
```

`bulk-add-nodes` and `bulk-add-edges` accept:

- `.json` files containing an array of objects
- `.jsonl` files containing one JSON object per line

### Direct tabular import from CSV and Parquet

Import nodes from CSV or Parquet:

```bash
uv run grapharrow import-nodes /tmp/grapharrow-db ./nodes.csv
uv run grapharrow import-nodes /tmp/grapharrow-db ./nodes.parquet --default-label Person
```

Import edges from CSV or Parquet:

```bash
uv run grapharrow import-edges /tmp/grapharrow-db ./edges.parquet
uv run grapharrow import-edges /tmp/grapharrow-db ./edges.csv \
  --source-label Person \
  --source-key external_id \
  --target-label Person \
  --target-key external_id
```

Simple tabular conventions:

- nodes:
  - `labels` column is optional when `--default-label` is provided
  - `labels` can contain multiple labels separated by `;`
  - every other column becomes a node property
- edges:
  - either `src_id`, `dst_id`, `rel_type`
  - or `src_key`, `dst_key`, `rel_type` plus explicit source/target label and key flags
  - every other column becomes a relationship property

## Real Object Storage

GraphArrow can work with URI-based database roots, including cloud object storage paths handled
through `pyarrow.fs`, for example:

- `s3://bucket/prefix/example-db`
- `gs://bucket/prefix/example-db`
- `az://container/prefix/example-db`

The repository already contains:

- local-disk integration tests
- S3-compatible integration tests via `moto`
- optional live integration tests for real S3, GCS, and Azure accounts

To run the live storage tests, configure one or more writable base URIs:

```bash
export GRAPHARROW_TEST_REAL_S3_URI="s3://my-grapharrow-tests/bootstrap"
export GRAPHARROW_TEST_REAL_GCS_URI="gs://my-grapharrow-tests/bootstrap"
export GRAPHARROW_TEST_REAL_AZURE_URI="az://grapharrow-tests/bootstrap"
make test-integration-live
```

See [Real Object Storage Testing](docs/real-object-storage-testing.md) for details.

GraphArrow can now also build cloud filesystem clients from explicit settings when needed.
The most relevant environment variables are:

- S3:
  - `GRAPHARROW_S3_ACCESS_KEY`
  - `GRAPHARROW_S3_SECRET_KEY`
  - `GRAPHARROW_S3_SESSION_TOKEN`
  - `GRAPHARROW_S3_REGION`
  - `GRAPHARROW_S3_ENDPOINT`
  - `GRAPHARROW_S3_SCHEME`
  - `GRAPHARROW_S3_ANONYMOUS`
- GCS:
  - `GRAPHARROW_GCS_ANONYMOUS`
  - `GRAPHARROW_GCS_ACCESS_TOKEN`
  - `GRAPHARROW_GCS_CREDENTIAL_TOKEN_EXPIRATION`
  - `GRAPHARROW_GCS_PROJECT_ID`
  - `GRAPHARROW_GCS_ENDPOINT`
  - `GRAPHARROW_GCS_SCHEME`
- Azure:
  - `GRAPHARROW_AZURE_ACCOUNT_NAME`
  - `GRAPHARROW_AZURE_ACCOUNT_KEY`
  - `GRAPHARROW_AZURE_SAS_TOKEN`
  - `GRAPHARROW_AZURE_CLIENT_ID`
  - `GRAPHARROW_AZURE_CLIENT_SECRET`
  - `GRAPHARROW_AZURE_TENANT_ID`
  - `GRAPHARROW_AZURE_BLOB_STORAGE_AUTHORITY`
  - `GRAPHARROW_AZURE_BLOB_STORAGE_SCHEME`
  - `GRAPHARROW_AZURE_DFS_STORAGE_AUTHORITY`
  - `GRAPHARROW_AZURE_DFS_STORAGE_SCHEME`

If these are not set, GraphArrow falls back to the default credential resolution provided by
`pyarrow.fs` and the underlying cloud provider environment.

## Docker

The repository includes two Docker images:

- `Dockerfile` for a lean runtime-style image with runtime dependencies only
- `Dockerfile.dev` for development and test workflows

Build the runtime image:

```bash
docker build -t grapharrow:runtime .
```

Run the runtime image:

```bash
docker run --rm -p 8000:8000 -e GRAPHARROW_DATABASE_PATH=/data/grapharrow grapharrow:runtime
```

Build the development image:

```bash
docker build -f Dockerfile.dev -t grapharrow:dev .
```

## Current Status

The codebase already includes a working bootstrap slice:

- project metadata and tooling are configured with `uv`;
- the package is wired through `app.py` with settings, adapters, repositories, and use cases;
- the embedded API is exposed through the interfaces layer while keeping stable top-level imports;
- a minimal CLI exists for database bootstrap, inspection, verification, and recovery;
- a sync REST server exists for health checks, query execution, graph writes, and admin operations;
- a minimal Bolt 4.4 server is available for direct Neo4j-driver access on the bootstrap path;
- immutable catalog and snapshot metadata are persisted through staged manifests and a current snapshot pointer;
- Parquet-backed node, edge, node-label, and adjacency storage is implemented for the bootstrap engine;
- the official `openCypher` grammar is vendored and used for parsing through ANTLR;
- a useful executable Cypher subset is available end-to-end (`CREATE`, `MATCH`, `WHERE`, `RETURN`, `ORDER BY`, `SKIP`, `LIMIT`, connected multi-pattern `MATCH`, and bounded variable-length traversals);
- integration tests cover local disk, REST, Bolt, and S3-compatible storage via `moto`.

The most important remaining work is deeper Cypher coverage, richer Bolt semantics,
more production-ready object-storage behavior, and continued cleanup/documentation of
the compiler/executor internals.

## Architecture Notes

- [Query Execution Architecture](docs/query-execution-architecture.md) explains how the vendored `openCypher` grammar compiles into GraphArrow's internal query model, how that model maps to PyArrow-backed execution, and how the same engine is surfaced through the embedded API, REST, and Bolt.
- [Cypher Engine Roadmap](docs/cypher-engine-roadmap.md) captures the plan to make the engine more Arrow-native, identifies the query classes where GraphArrow may outperform Neo4j, and outlines the path toward a true compiler, executor, and optimizer stack.
- [MCP Interface Design](docs/mcp-interface-design.md) defines the first MCP surface for LLM-oriented usage, starting from `query`, `write_cypher`, `get_schema`, `get_constraints`, and `get_indexes`.
- [Real Object Storage Testing](docs/real-object-storage-testing.md) describes the optional live integration suite for real S3, GCS, and Azure backends.
