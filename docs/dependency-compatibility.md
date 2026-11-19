# Dependencies and Installation

GraphArrow requires Python 3.11 or later and PyArrow 26.0.1 or later. Runtime,
development and optional benchmark requirements are defined in
[pyproject.toml](../pyproject.toml). The [lockfile](../uv.lock) records resolved
versions, including differences between Python versions and platforms.

## Development environment

Install the locked runtime and development dependencies from the repository root:

```sh
uv sync --locked
```

For comparisons with the optional DuckDB and Polars backends, include the
benchmark dependency group:

```sh
uv sync --locked --group benchmark
```

To run provider SDK tests and type checks with all cloud integrations:

```sh
uv sync --locked --all-extras
uv run --frozen --all-extras pytest -m "not live_storage and not neo4j_reference"
uv run --frozen --all-extras mypy src
```

Development dependencies include HTTP test clients, the Neo4j comparison driver,
and local cloud-storage emulators. They are not runtime requirements.
GraphArrow implements ANA tools itself and does not install the reference ANA
server or the Neo4j driver as runtime dependencies.

## Runtime installation

The default Docker image installs locked runtime dependencies and the `cloud`
extra without the development group. The equivalent checkout installation is:

```sh
uv sync --locked --no-dev --extra cloud
```

An installed wheel uses the runtime requirement ranges declared in its package
metadata; it does not enforce the repository lockfile. Use the lockfile when
an exact, reproducible dependency environment is required.

## Optional cloud SDKs

Cloud data reads use PyArrow; publishing and reading the database head uses the
provider SDK to preserve conditional-write semantics. These SDKs are loaded only
when their provider is selected. Local storage and the default server installation
do not require them. Missing SDKs produce an error naming the required extra.

| Extra | SDKs |
| --- | --- |
| `s3` | boto3 |
| `azure` | azure-storage-blob, azure-identity |
| `gcs` | google-cloud-storage |
| `cloud` | All three provider integrations |

```sh
uv sync --locked --extra s3
uv run --frozen --extra s3 grapharrow-server --path s3://my-bucket/my-db
pip install 'grapharrow[s3]'
# Before PyPI publication, use a downloaded release wheel instead:
pip install './grapharrow-<version>-py3-none-any.whl[s3]'
```

The default Docker image includes all provider SDKs. Their Python modules are
imported only when the corresponding provider is used, so local storage does not
load them. To build an image with only one provider SDK from the checkout:

```sh
docker build --build-arg STORAGE_EXTRA=s3 -t grapharrow:s3 .
```

`STORAGE_EXTRA` accepts a space-separated list and defaults to `cloud delta`.
An empty value builds an image with only Native and local storage dependencies.
The source-build option above regenerates locked requirements with the selected
extras. Release builds export `runtime-requirements.txt` with `--extra cloud --extra delta`
for the prebuilt image. Wheel installations select cloud SDKs and Delta separately.
Development environments using Moto also install boto3 as an emulator dependency;
that does not make it a dependency of the base GraphArrow package.

## Storage Backend

A **Storage Backend** defines how GraphArrow reads and writes graph snapshots,
publishes atomic commits, and performs maintenance. `native` is the default.
`delta` uses a Delta Lake transaction log for independent reader and writer processes.

Filesystem paths, S3, Azure Blob Storage, and Google Cloud Storage identify the
physical storage location. Filesystem adapters provide access to those locations.
Cypher, ANA, REST, Bolt, and the embedded API use the graph engine above this layer.
See [Storage Backends](storage-backends.md) for backend selection and guarantees.

## Storage compatibility

The dependency update does not introduce a new snapshot manifest or Parquet
layout. Reads still validate dataset schemas and use canonical values when a
property projection cannot preserve their types. This does not guarantee
compatibility with every externally generated Parquet schema or optional cloud
backend. See [Parquet Predicate Pushdown](parquet-predicate-pushdown.md) for
supported physical types and [object-storage testing](real-object-storage-testing.md)
for testing a configured cloud account.
