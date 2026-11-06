# Contributing to GraphArrow

GraphArrow is an experimental database. Changes to query semantics, snapshots,
public tool contracts and storage layout need focused correctness tests.

## Development environment

Use Python 3.11 or later, Git and uv:

```bash
uv sync --locked
make lint
make format-check
make typecheck
uv run pytest -m "not live_storage"
```

ANA is included in the standard installation. Its upstream source is pinned by
commit in `pyproject.toml` and `uv.lock`; no adjacent repository is required.

The local suite includes loopback HTTP/Bolt and simulated S3 tests. Real-cloud
tests are opt-in and require dedicated storage configuration; see
[Object Storage Testing](docs/real-object-storage-testing.md).

## Code and documentation

Write versioned code, comments, docstrings and documentation in English. Use
repository-relative links. Keep public guides and shareable technical analysis
in `docs/`; private plans, roadmaps, internal ADRs and exploratory notes belong
in ignored local directories and must not be force-added.

Preserve recorded benchmark measurements when editing presentation. New
performance claims need their workload, configuration and before/after evidence.
Do not run model-backed benchmarks or real-cloud tests as part of ordinary CI.

## ANA changes

Keep the upstream tool names, native schemas, aliases and HTTP envelopes.
Expose only implemented operations and test handle identity, empty results,
nulls, duplicates, limits and workflow isolation. The GraphArrow data path must
not create a Neo4j driver or silently send queries to another engine.

Run the [differential workflow suite](docs/ana-interface.md#validation) against
an empty disposable Neo4j database when changing ANA data semantics. Ordinary
local tests skip it when the reference URI is absent; the separate CI job runs
it against an isolated service. The fixture refuses preexisting graph data.

## Pull requests

Describe the triggering problem, the resulting behavior and relevant validation.
Call out public API/storage changes and known limits. Exclude credentials,
generated datasets, local plans and unrelated benchmark output.

The CI workflow checks Python 3.11 and 3.12, runs local tests and builds the
distributions. Builds and tests do not publish packages, images or releases.
