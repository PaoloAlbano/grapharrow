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

ANA is included in the standard installation and uses GraphArrow's own runtime,
tool schemas and descriptions. No reference ANA package or adjacent checkout is
required. The reference server is used only for optional HTTP compatibility
tests; see the [ANA interface guide](docs/ana-interface.md).

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

## Cypher changes

The parser is generated from `vendor/opencypher/GraphArrowCypher.g4`, which
imports the unchanged upstream `Cypher.g4`. To regenerate it, obtain the
ANTLR 4.13.2 complete JAR from the [ANTLR download page](https://www.antlr.org/download.html)
and use a compatible Java runtime:

```bash
make generate-cypher-parser ANTLR_JAR=/absolute/path/to/antlr-4.13.2-complete.jar
```

Commit the derived grammar and its generated lexer, parser, visitor and token
files together. Keep the original upstream grammar and its notices intact.
Java and the generator JAR are development tools, not runtime dependencies.

Run the unit and interface comparison suites for query changes. The optional
Neo4j differential suite shares the reference connection settings used by the
ANA suite. Point them at an empty disposable database only; the fixtures refuse
existing data and remove their own test nodes afterwards:

```bash
export GRAPHARROW_TEST_NEO4J_URI=bolt://127.0.0.1:17687
export GRAPHARROW_TEST_NEO4J_USER=neo4j
# Set GRAPHARROW_TEST_NEO4J_PASSWORD to the disposable service's password.
uv run pytest tests/integration/test_cypher_reference.py
```

The reference service is used by tests only. GraphArrow executes queries through
its own engine and does not require the Neo4j driver in a standard installation.

## Pull requests

Describe the triggering problem, the resulting behavior and relevant validation.
Call out public API/storage changes and known limits. Exclude credentials,
generated datasets, local plans and unrelated benchmark output.

The CI workflow tests Python 3.11 and 3.15 on pull requests and Python 3.11
through 3.15 on main. Lint, formatting and typechecking run separately, once
each. Real-cloud tests and benchmarks are excluded from ordinary CI.

Open issues through the bug, performance or feature request forms. Include a
minimal reproduction with synthetic data and sanitized logs; do not include
credentials, private endpoints or personal information. The pull request
template asks for the problem, resulting behavior and relevant validation.

The [CI and release guide](docs/releases.md) explains distribution checks and
tag-driven releases. Main and PR checks build and test artifacts; pushing a
matching `v*` tag runs full validation and then publishes GitHub Release assets
and the two Docker architectures. PyPI publication is not configured.
