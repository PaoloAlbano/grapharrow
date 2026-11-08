# Columnar execution for composable Cypher reads

Eligible composable read queries scan selected per-label property columns from
Parquet, filter Arrow batches and project expressions before constructing public
Python result rows. They do not load canonical node/edge tables or decode the
whole graph into records. All query interfaces use the same Cypher dispatcher.

## Coverage

The initial pattern must be one nonoptional node with one label. Subsequent
clauses can be projection stages, including multiple `WITH` boundaries, scalar
aliases, `CASE`, `coalesce`, comparisons, boolean/null predicates, checked numeric
addition/subtraction/multiplication and selected string functions. Entity aliases
can pass through intermediate `WITH` clauses when final results are scalar.
Ordering, `SKIP` and `LIMIT` remain at their original clause boundaries; a `WITH`
predicate still runs after that clause's window.

Exact inline properties and eligible conjuncts of the initial `MATCH WHERE` are
offered to the Parquet scanner. The complete Arrow predicate is always applied
after reading, including when scanner filtering is disabled or unavailable.
Later `WITH` filters run on Arrow batches; they are not moved across windows or
aggregation boundaries into the initial reader.

After a prefix projects all entity bindings into scalars, more complex projections,
grouping and `DISTINCT` can use the existing row executor over that reduced data.
Unions whose branches qualify share one captured snapshot and retain their
existing column alignment and duplicate semantics.

Traversal, optional patterns, correlated subqueries, entity-valued results and
transaction-local graph overlays retain the general runtime. This optimization
does not lower every composable query into Arrow or provide streaming execution.
The existing planner and specialized direct execution paths remain available.

## LIMIT and bounded ordering

Small `ORDER BY ... SKIP ... LIMIT` windows select the best `SKIP + LIMIT`
candidates and sort only that subset. A unique input ordinal preserves ties;
mixed directions and per-key null placement remain unchanged. Larger windows
retain full sorting. The shared Arrow selection also serves eligible existing
planner/direct queries; the row pipeline uses bounded candidate retention.
Grouping and `DISTINCT` still precede the window and may require complete state.

Safe scalar projections that are not needed for ordering are evaluated after
selection. For small first ordered windows, the scan reads predicate/order keys
first and retrieves remaining properties in batches for the selected node IDs
from the same snapshot. This late property read currently applies to windows of
at most 1,024 candidates. Fallible arithmetic and conversions remain eager so
discarded rows cannot hide established expression errors.

Without ordering, an eligible first projection window can consume Parquet
batches until enough complete `MATCH` residuals have passed. It retains only the
requested rows after `SKIP`, and closes the iterator. A `WITH WHERE` still runs
after that clause's window. This early stop requires validated native encodings
and total expressions; other shapes keep the full scan. Parquet pages, row
groups and reader prefetch can read more bytes than the returned row count.

These optimizations do not make arbitrary pattern matching, grouping or public
query results streaming. Sorted queries still inspect all relevant ordering
keys. Set `GRAPHARROW_DISABLE_CYPHER_LIMIT_OPTIMIZATIONS=1` to compare against
full sorting, eager projections and complete property-table reads.

## Exactness and fallback

Projection schemas are inspected through Parquet metadata before scanning data.
Only native scalar encodings qualify. JSON-encoded values, lossy mixed numeric
projections, legacy projections without encoding metadata and reserved identity
collisions use canonical records instead. Missing properties become null columns.

Unsupported type combinations and kernel failures fall back on the same captured
snapshot, including if another writer has published a new snapshot meanwhile.
Checked arithmetic avoids integer wraparound. A failing eager Arrow evaluation
of an unselected `CASE`/`coalesce` operand falls back to the lazy row evaluator.
Unicode case conversion outside ASCII and floating NaNs also retain that evaluator.
Eligibility checks precede reader predicates so an unsafe partial filter cannot
hide a type error from the original expression.

Set `GRAPHARROW_DISABLE_CYPHER_COLUMNAR=1` to use the general row runtime for
these queries. `GRAPHARROW_PARQUET_FILTER_PUSHDOWN_ENABLED=false` disables reader
filtering while keeping eligible Arrow projections and residual filtering.
These switches are useful for diagnostics and result comparisons.

## Reproducing measurements

The benchmark creates dedicated synthetic databases and refuses to overwrite
existing paths. It compares the general row runtime, the columnar path and the
columnar path with reader filtering disabled. One selective query also compares
an equivalent existing direct/planner query. An optional `--baseline-source`
points to a separate source checkout for a true before/after comparison.

```bash
uv run python scripts/benchmark_cypher_columnar.py \
  --sizes 1000,10000,100000 --repeats 3 \
  --directory /tmp/grapharrow-columnar-data \
  --output /tmp/grapharrow-columnar-results.json
```

Each workload/mode/size uses a fresh subprocess. Reports contain the first call,
warm latency samples, process peak RSS, Arrow allocator peak, projected columns,
reader predicates and result digests. All modes must return the same digest.
Peak RSS includes the Python runtime and dependencies; it is not retained query
memory. OS page caches are uncontrolled, so the first call is not a cold-disk
measurement. The benchmark runs embedded queries without transport overhead.

Workloads include selective projections, filters after scalar `WITH`, `CASE`
grouping, broad projections and a correlated traversal control that stays on the
general runtime. Synthetic results do not establish universal speedups or a
comparison with another database engine.

Use `--suite limits` to compare window optimizations enabled/disabled and an
optional separate baseline source. That suite covers unordered and filtered
prefixes, late payload retrieval, mixed ordering through `WITH`, an existing
planner query and a large-window control. Give each run a fresh `--directory`.
The direct Parquet batch reader prunes row groups using safe predicates and
always retains the complete per-batch residual; it does not rely on row-level
scanner filtering for either prefix counting or late property retrieval.

See the [columnar correctness tests](../tests/unit/test_cypher_columnar.py) and
[query execution architecture](query-execution-architecture.md).
