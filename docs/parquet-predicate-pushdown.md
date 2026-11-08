# Parquet Predicate Pushdown

Eligible Cypher property filters reach the Parquet dataset scanner before
property projection rows are returned to the executor. The same query use
case serves embedded Python, REST, Bolt, and MCP Cypher.

## Execution path

The executor translates eligible scalar conjuncts into storage-neutral
`ScanPredicate` values. Graph use cases and repositories pass them together
with the required columns and immutable snapshot file references. The Parquet
adapter validates the predicates against the physical dataset schema and
passes an Arrow expression to `dataset.scanner(filter=...)`.

The existing binding-table filters remain authoritative and run afterward.
A physical hash index, when eligible, is still preferred; index construction
always reads the complete projection and cannot cache a filtered subset.
Per-query projection caches include predicates in their keys, keeping reads
for different aliases and values separate.

The scanner can skip row groups using their Parquet statistics. When pruning
is unavailable, it applies the filter to the scanned batches. This is the
behavior described by the official
[Arrow dataset scanner documentation](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html#pyarrow.dataset.Dataset.scanner).

## Eligible filters

The initial implementation covers:

- inline node property comparisons and eligible single-node `WHERE` conjuncts;
- inline relationship property comparisons on typed property projections;
- constant or bound-parameter equality, inequality, and ordered comparisons;
- scalar `IN`, `IS NULL`, `IS NOT NULL`, and property-existence predicates.

For example:

```cypher
MATCH (d:Device)
WHERE d.value >= $lower AND d.value < $upper
RETURN d.name
```

Node-only `WHERE` filters reach eligible scans with the planner enabled or
disabled. More complex traversal shapes retain their existing access paths.
Queries still need a suitable property projection and the required fields.
Full-entity canonical reads are not converted into projection-only scans.

Supported comparison types are native strings, booleans, integers within
compatible bounds, and finite double-precision scalar constants. Implicit
numeric conversions, missing or ambiguous fields, unsupported Arrow kernels,
expressions, property-to-property comparisons, and disjunctions or negations
remain on the existing execution path. Supported conjuncts of an `AND` may
be pushed independently; an `OR` is not partially pushed.

Escaped physical property names are resolved against the file schema.
Requested output columns and filter columns can differ. Empty filtered
results retain their schema and do not trigger an alternate-label read.
Scanner filter errors for unsupported Arrow operations retry an unfiltered
scan; filesystem and I/O errors are not silently ignored.

Floating-point `!=` is deliberately excluded. With the tested PyArrow 23,
a Parquet row group containing `1.0` and NaN can have minimum and maximum
statistics of `1.0`. Pushing `value != 1.0` can prune the NaN match that the
existing residual Arrow comparison retains. A regression test verifies this
case. Non-finite literal constants also stay on the residual path.

## Configuration

Pushdown is enabled by default. Disable it for comparison or diagnostics:

```sh
export GRAPHARROW_PARQUET_FILTER_PUSHDOWN_ENABLED=false
```

The setting is read when constructing the application. It does not change
query results or the physical file layout.

## Snapshot and interface boundaries

Reads use the files of the captured snapshot; filtering does not reload the
current head. Explicit Bolt transaction overlays keep their in-memory path,
including uncommitted changes. ANA workflows continue to read their pinned
in-memory tables: this change does not make ANA snapshots lazy or shared.
No production row-group sizing, sorting, codec, manifest, or publication
protocol changes are part of the reader optimization.

Current Cypher execution still loads the required canonical state before
reading property projections. The optimization reduces projection candidates
and can reduce projection I/O; it does not eliminate every canonical read.
Large gains depend on selective filters and useful row-group statistics.

The [pushdown benchmark runner](../scripts/benchmark_parquet_pushdown.py) compares
enabled and disabled scanner filtering, both Cypher planner modes, and temporary
copies with different row-group layouts. It records candidate counts, file bytes
delivered to the reader, and query latency. Delivered bytes do not measure
physical disk reads or network traffic. Experimental copies do not alter the
production snapshot layout.

```sh
uv run --locked --group dev python scripts/benchmark_parquet_pushdown.py \
  --nodes 100000 --repeats 15 --row-group-size 4096 \
  --output .benchmarks/parquet-pushdown.json
```
