# Shared Snapshot Adjacency

Measured on November 7, 2026, with Python 3.12.11 and PyArrow 26.0.1 on
macOS arm64. The baseline uses a workflow-local ANA adjacency index. The shared
implementation moves position indexing into the graph service and shares prepared indexes
between participating ANA workflows and Cypher queries in one application.
Both runs use the same dependencies, runner and immutable fixture.

## Method

The [runner](../../scripts/benchmark_shared_adjacency.py) creates 25,000
`Device` nodes and 75,000 `LINK` edges, with three outgoing edges per node.
Edges have integer weights 0, 1 and 2. A declared equality index on node names
keeps seed lookup selective. Queries cover outgoing, incoming and two-hop
traversals, a relationship-property filter, and a broad count control.
Two-hop outputs retain duplicate endpoints from different paths.

Each interface/query configuration gets a fresh application or server. The
first call is recorded separately, followed by two warmups and 21 timed calls.
The same query and parameters repeat within that configuration. Tables report
warm medians in milliseconds. Raw samples and nearest-rank p95 values are in
the [supporting measurements](shared-adjacency.json).

Native times include the public Cypher executor and result conversion. REST
uses FastAPI TestClient, including request/response and JSON processing but no
TCP HTTP transport. Bolt uses the actual Neo4j driver and loopback socket
server, including result consumption. Authentication/connection verification
and server setup are outside the timer. The explicit Bolt case holds one
transaction open across calls; constructing its fully materialized base state
is outside the timer. These are not measurements of opening a new transaction
for every query.

The ANA case first warms the same application's outgoing Cypher query, then
keeps one ANA workflow open to retain schema discovery. Each timed call opens
a fresh workflow, resolves a seed, expands, fetches and closes it. The previous
implementation builds another adjacency index per workflow. The common engine
reuses the prepared entry from Cypher. Snapshot/property loading and node-map
construction still run for each workflow. Optional ANA result caching is off.

## Results

| Interface | Workload | Before (ms) | Shared engine (ms) |
| --- | --- | ---: | ---: |
| Native Cypher | outgoing | 11.968 | 10.160 |
| Native Cypher | incoming | 11.752 | 10.083 |
| Native Cypher | two hop | 16.764 | 14.462 |
| Native Cypher | property | 15.463 | 11.703 |
| Native Cypher | broad count | 15.827 | 14.794 |
| REST TestClient | outgoing | 12.736 | 11.978 |
| REST TestClient | incoming | 13.725 | 12.087 |
| REST TestClient | two hop | 18.956 | 17.152 |
| REST TestClient | property | 15.107 | 13.417 |
| REST TestClient | broad count | 17.692 | 19.188 |
| Bolt autocommit | outgoing | 13.338 | 13.372 |
| Bolt autocommit | incoming | 13.746 | 12.146 |
| Bolt autocommit | two hop | 20.464 | 17.221 |
| Bolt autocommit | property | 16.758 | 13.312 |
| Bolt autocommit | broad count | 20.033 | 18.408 |
| Open Bolt transaction | outgoing | 48.577 | 40.176 |
| Open Bolt transaction | incoming | 47.413 | 41.741 |
| Open Bolt transaction | two hop | 58.244 | 45.994 |
| Open Bolt transaction | property | 98.374 | 39.997 |
| Open Bolt transaction | broad count | 19.446 | 20.793 |
| New ANA workflow | outgoing | 34.068 | 28.209 |

The native two-hop query decreases from 16.764 to 14.462 ms, and the same
query over Bolt from 20.464 to 17.221 ms. The already-open Bolt transaction's
property case decreases from 98.374 to 39.997 ms: candidate selection avoids
constructing bindings for unrelated canonical relationship properties. The
fresh ANA workflow case decreases from 34.068 to 28.209 ms with the shared
entry already prepared.

Improvements are not uniform. Bolt outgoing latency is essentially unchanged
(13.338 to 13.372 ms). The broad REST control rises from 17.692 to 19.188 ms,
and the broad open-transaction control from 19.446 to 20.793 ms. Broad
frontiers bypass selective indexing; node loading, existing joins, allocation,
transport and local measurement variability remain visible. Each configuration
has one worker run, so these observations do not establish statistical
significance, a production latency bound or a universal speedup.

## First use and memory

Construction is lazy and shared, not eliminated. First-call times include
parsing, cold property-index work and execution as well as possible adjacency
construction. For example, native incoming changes from 45.87 to 49.23 ms,
and native two-hop from 31.93 to 37.79 ms. Some other first-call observations
are lower; these samples do not isolate index-build cost. The first call in a
new application can remain slower than an already prepared lookup.

After the Cypher/ANA reuse case, the application retains one entry with one
completed build. Its conservative cache reservation is 2,111,366 bytes
(2.01 MiB), including file keys. Index capacity is 2,108,794 bytes, and the
retained-object estimate is 2,008,104 bytes. These are retained-state estimates,
not sampled peak RSS. Temporary sorting/gathering buffers, canonical graph
reads and concurrently pinned owners are outside that cache reservation.

The application LRU has a default 32 MiB ceiling, configurable with
`GRAPHARROW_ADJACENCY_INDEX_MAX_BYTES`; `0` disables this acceleration across
frontends. Each participating ANA owner still reserves a full index share
inside its own existing quotas, subject to its separate 8 MiB eligibility
ceiling. Oversized admissions use existing execution paths. Bounded negative
entries avoid repeated type-name scans for a snapshot that cannot fit.

Closing ANA releases its reference and byte charge; the LRU can retain a
prepared entry for subsequent workflows and Cypher. Evicted entries can stay
alive while active owners pin them. Separate applications/processes have
separate caches, even when they use the same database directory.

## Semantics and scope

The common adapter stores canonical row positions by type and endpoint and
retains no canonical property tables, handles, entity owners or lineage.
Immutable edge-file metadata, including order and checksums, forms the cache
key. Unchanged files can reuse an entry across snapshots; changed files get a
new entry. The existing Parquet layout and publication protocol are unchanged.

Eligible fixed-hop Cypher execution narrows candidate edges before its existing
joins, with planner enabled or disabled, including optional and multi-part
expansions. Joins retain binding multiplicity and relationship uniqueness.
Property projections preserve native schemas and residual filters, then retain
only selected edge IDs. Wide frontiers, full relationship returns, specialized
variable-length execution and ambiguous imported aliases keep established
paths. Node reads and property projection I/O are not eliminated.

Explicit transactions reuse the shared entry only for their exact published
base edge table. Uncommitted overlays use a private index, invalidated after
mutations and released on commit/rollback. No overlay enters the published
cache. Empty incoming fallbacks retain the relationship-type filter, and
empty fallback paths retain requested field names for valid projection.

All 21 before/after result digests match. For the five Cypher workloads,
columns and rows also match across native, REST, Bolt autocommit and Bolt
transaction modes. ANA fetches return the expected three target names.
Regression tests additionally check shared single-flight builds, independent
handle ownership, eviction with live owners, unchanged-file reuse, quota
fallbacks, negative admission caching, private overlays, rollback, pinned
snapshots and releasing canonical query tables without waiting for cyclic GC.
The public interface comparison includes embedded, REST, MCP, Bolt and ANA.

## Reproduction

The recorded comparison used two local source snapshots identified by hashes
in the supporting JSON. The command below recreates the fixture and measures
the checked-out implementation; it does not reconstruct the historical baseline.
For subsequent runs on the same fixture, omit `--build-fixture`:

```sh
uv run --group dev python scripts/benchmark_shared_adjacency.py \
  --database .benchmarks/shared-adjacency \
  --build-fixture --nodes 25000 --warmups 2 --repeats 21 \
  --output .benchmarks/shared-adjacency.json
```

Bolt benchmarking requires permission to bind local loopback ports. Use
`--interfaces native` for native-only query measurements; the fresh ANA
workflow comparison still runs. Set
`GRAPHARROW_ADJACENCY_INDEX_MAX_BYTES=0` before starting an application to
exercise the shared engine's fallback paths. Raw results include source
hashes, full samples, first-call observations and cache diagnostics.
