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

Development dependencies include HTTP test clients, the Neo4j comparison driver,
and local cloud-storage emulators. They are not runtime requirements.
GraphArrow implements ANA tools itself and does not install the reference ANA
server or the Neo4j driver as runtime dependencies.

## Runtime installation

The Docker image installs locked runtime dependencies without the development
group. The equivalent checkout installation is:

```sh
uv sync --locked --no-dev
```

An installed wheel uses the runtime requirement ranges declared in its package
metadata; it does not enforce the repository lockfile. Use the lockfile when
an exact, reproducible dependency environment is required.

## Storage compatibility

The dependency update does not introduce a new snapshot manifest or Parquet
layout. Reads still validate dataset schemas and use canonical values when a
property projection cannot preserve their types. This does not guarantee
compatibility with every externally generated Parquet schema or optional cloud
backend. See [Parquet Predicate Pushdown](parquet-predicate-pushdown.md) for
supported physical types and [object-storage testing](real-object-storage-testing.md)
for testing a configured cloud account.
