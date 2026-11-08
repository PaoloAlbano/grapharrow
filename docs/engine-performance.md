# Engine Read and Write Optimizations

ANA, embedded Cypher, REST, and Bolt use the same graph use cases. These
optimizations preserve their existing snapshot and result contracts.

## Discovery without result caching

ANA discovery reads narrow property columns and materializes entity payloads
only for selected results and alternatives. New node-property projections
carry field metadata identifying native scalar values, JSON-encoded values,
and lossy numeric conversions. Historical projections remain readable.
Canonical JSON is used when a projection cannot preserve Python value types,
missing properties, explicit nulls, or reserved-column identities.

String value search uses Arrow filters for exact matching and for ASCII
case-insensitive searches. Unicode case-insensitive searches retain Python
lowercasing semantics. Limits select the best candidates with stable ties.
These paths work with the optional workflow result cache disabled.

## Aggregations and ordered limits

ANA aggregates maintain counts, exact distinct identities, and scalar
aggregate state rather than lists of all binding rows in each group. Numeric
summation preserves the running interpreter's built-in `sum` behavior.
CPython 3.12 uses compensated floating-point accumulation, following the
[built-in implementation](https://github.com/python/cpython/blob/3.12/Python/bltinmodule.c).
On Python 3.13 and later the compatibility path retains numeric scalars for
`sum` and `avg` and delegates to that interpreter's built-in implementation.

Ordered, limited projections select the best K candidates progressively.
Single-key ascending and descending sorts use their corresponding heap
selection operations. Multiple keys preserve direction, null ordering, and
stable ties. NaNs, untyped historical projections, encoded keys, and grouped
outputs retain the full stable-sort path.

Exact distinct sets, the number of groups, complete branch memberships, and
unlimited outputs can still grow with input size. Retained-state quotas do
not impose a general limit on temporary process memory.

## ANA seed selection and ranked discovery

ANA pattern start filters now select IDs before allocating entity payloads.
Native scalar masks and declared equality lookups run first when exact; a
predicate that needs Python semantics stops the pushdown prefix. Remaining
predicates evaluate over narrow properties, with canonical fallbacks for
missing, legacy or non-reconstructable projections. Input-handle scope, node
order, error ordering and complete traversal branch counts remain intact.

Entity resolution uses Arrow to rank eligible ASCII rows and retain the
necessary finalists from both the hit and alternative rankings. Exceptional
rows remain available to the original Python scorer, which rescans the selected
union. Small/unsupported cases keep the original scan. No persistent text index
or new cache reservation is introduced. These are ANA tool semantics; Cypher
continues to use its own predicate and projection operators on the shared graph
service. See [ANA Seed Filtering and Ranked Discovery](benchmarks/ana-seed-discovery.md).

## Shared snapshot adjacency

Selective fixed-hop Cypher expansions use the same compact adjacency index as
ANA. The graph service owns an application-local LRU keyed by the ordered
immutable canonical edge-file metadata, including checksums. The index retains
canonical row positions grouped by relationship type and endpoint. It retains
no canonical payload tables, entity objects, handles or lineage. Construction
is lazy, single-flight and retryable; outgoing and incoming structures publish
together. Unchanged edge files can reuse an entry across snapshots.

Cypher gathers candidate edges before joins in eligible planned and fallback
fixed-hop paths, including multi-part and optional expansions. Its existing
joins restore binding multiplicity, relationship exclusions and optional null
rows. Property projections keep their native schemas and residual filters,
then intersect with selected edge IDs. Wide frontiers use the established
vectorized path. Full relationship returns, specialized variable-length paths,
ambiguous imported escaped names and reads without complete canonical edges
retain their existing execution paths. Property projection reads and node
materialization can therefore remain significant costs.

All Cypher frontends use this engine: embedded, REST, MCP and Bolt. Within one
application, ANA and Cypher can share the same prepared index. Separate
applications and server processes have separate caches. Explicit transactions
reuse a published entry only while their exact base edge table is unchanged;
overlays use a private index, invalidated after each mutation and released on
commit or rollback. Uncommitted topology never enters the published cache.

```sh
export GRAPHARROW_ADJACENCY_INDEX_MAX_BYTES=33554432
```

The default 32 MiB ceiling charges conservative index reservations and manifest
keys in the LRU. `0` disables this acceleration across frontends. Oversized
reservations use the existing paths; bounded negative entries avoid repeatedly
scanning type names after a failed admission. ANA additionally reserves a
complete share inside each participating workflow's existing quotas, with its
own 8 MiB eligibility ceiling. Closing a workflow releases that charge, while
the application LRU may keep the prepared index. Evicted entries can remain
pinned by active workflows or queries; this cache ceiling is not a process RSS
limit. Node tables and property projections remain independently loaded.

No storage migration or new persistent dataset is required. The
[shared-adjacency benchmark](benchmarks/shared-adjacency.md) reports first-call
costs, reuse across workflows and measured limits.

## Physical equality indexes

Case-sensitive, single-property `range` or `lookup` declarations on node
labels can back lazy physical hash indexes. Supported indexed values are
native strings, integers, and booleans. The first eligible lookup builds an
index from the label's property projection; later lookups reuse it. ANA
`node_search`, exact string `value_search`, eligible pattern seed equality, and Cypher property
projections use the same application-local index store. Cypher supports both
inline property matches and pushed-down constant equality predicates.

```cypher
CREATE INDEX device_name FOR (d:Device) ON (d.name)
MATCH (d:Device {name: $name}) RETURN d.name
```

Indexes are keyed by immutable projection file metadata and property name.
Changes to the projection produce another key; unchanged files can reuse
the same index across snapshots. ANA still pins its workflow snapshot and
creates its own handles. Explicit Bolt transaction overlays bypass these
indexes and read their uncommitted state through the transaction path.
Dropping a declaration stops new queries from selecting it. Existing ANA
workflows continue with their pinned catalog and snapshot.

Index memory has a separate per-application budget. It includes retained
Arrow projection buffers and estimated Python index storage. The store evicts
least-recently-used entries and falls back to scans when an index cannot fit.
This budget is additional to ANA workflow quotas and is per process.

```sh
export GRAPHARROW_PROPERTY_INDEX_MAX_BYTES=33554432
```

The default is 32 MiB; zero disables physical index construction. These are
in-memory indexes, rebuilt after restart. Historical projections without the
encoding metadata fall back to scans; `compact` rewrites them in the current
format. Composite, relationship-property, range-search, text, full-text, and
vector indexes are not physically implemented by this equality access path.
Full-entity Cypher shapes can also retain their canonical scan path.

## Immutable file reuse during writes

Publication compares the previous and candidate records. Canonical node or
edge files are retained when those records are unchanged. Derived files are
retained by label or relationship type when their inputs are unchanged.
Label membership files can survive property changes, and adjacency files
can survive edge-property changes when topology is unchanged.

The new manifest references existing immutable files directly. Their paths,
checksums, and contents remain unchanged. Vacuum retains every file referenced
by the current manifest, even when its filename contains an older snapshot
ID. Compaction deliberately rewrites all datasets and rebuilds projections.
The manifest format and publication pointer protocol are unchanged.

This reduces Parquet writes and checksum work. A changed canonical node or
edge dataset is still rewritten in full, and write use cases still load and
validate the graph. This is not a row-level delta storage format or a WAL.
Writer coordination and recovery limits remain described in
[Transactions and Sessions](transactions.md).

## Cypher syntax reuse

Each application caches read-only syntax trees under their exact query text.
Parameter binding, compilation, and planning run on every execution; results,
parameters, and schema-dependent plans are not stored in the syntax cache.
The shared cache also serves explicit Bolt transactions.

```sh
export GRAPHARROW_CYPHER_PARSE_CACHE_MAX_ENTRIES=128
export GRAPHARROW_CYPHER_PARSE_CACHE_MAX_SOURCE_BYTES=262144
```

Either setting can be zero to disable reuse. Entries are evicted in
least-recently-used order. Queries larger than 16 KiB of UTF-8 text bypass
the cache, and parse errors are not cached. The source-byte setting limits
retained query text, not the exact memory occupied by syntax-tree objects.

## Measurement tools

The published measurements cover [shared adjacency](benchmarks/shared-adjacency.md)
and [ANA seed selection and discovery](benchmarks/ana-seed-discovery.md). Their
dated results include fixtures, timing boundaries, controls and limitations.

The following runners can measure the checked-out implementation. They write
results locally; running a measurement does not add its output to the repository.
Use `--help` for options and choose output filenames that preserve earlier runs.

| Runner | Scope |
| --- | --- |
| [Shared adjacency](../scripts/benchmark_shared_adjacency.py) | Native Cypher, REST, Bolt, open transactions and new ANA workflows |
| [Seed filtering and discovery](../scripts/benchmark_ana_seed_discovery.py) | Uncached ANA patterns and entity resolution through native and HTTP calls |
| [Read cache](../scripts/benchmark_ana_read_cache.py) | Cold and repeated discovery with the optional cache enabled and disabled |
| [Call bookkeeping](../scripts/benchmark_ana_call_state.py) | Tool calls as retained handle counts grow, including admission rollback |
| [Traversal stress](../scripts/benchmark_ana_traversals.py) | Selective and high-degree ANA traversals with concurrent workflows |
| [Engine operations](../scripts/benchmark_engine_optimizations.py) | Discovery, aggregation, ordered limits, syntax parsing and publication |
| [Parquet pushdown](../scripts/benchmark_parquet_pushdown.py) | Scanner filtering, row-group experiments and end-to-end Cypher controls |

Compare like-for-like environments and verify result digests before comparing
latency. Microbenchmarks, temporary row-group experiments and dependency-upgrade
checks do not establish production latency or a recommended storage layout.
