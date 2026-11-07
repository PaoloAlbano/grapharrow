# Data Access Backend Evaluation

## Reproduce the comparison

Run `make benchmark-backends` from the repository root. It uses the locked
`benchmark` dependency group and generates a shared synthetic Parquet dataset.
The runner checks row counts and checksums across the four backends before
writing JSON and Markdown reports to `.benchmarks/backend_evaluation/`.

For a small run:

```bash
uv run --locked --group benchmark python scripts/benchmark_backends.py \
  --nodes 2000 --edges 8000 --warmups 1 --repeats 2
```

The [recorded benchmark](benchmarks/data-access-backend-benchmark.md) uses
300,000 nodes and 1,200,000 edges. Timings vary with the environment and workload.

## Goal

GraphArrow stores graph data in Parquet and should work well with local filesystems and object storage such as S3.
Before going deeper into the execution engine, it is useful to compare the main candidate libraries for reading,
filtering, projecting, and processing that data:

- `pyarrow`
- `pandas`
- `polars`
- `duckdb`

The main question is not only "which library is faster?".
The main question is:

> Which backend gives us the best foundation for a graph database that owns its own storage model and execution engine?

## Evaluation Criteria

The comparison below focuses on:

- Native fit with Parquet and Arrow memory
- Fit with S3 and object storage
- Control over execution internals
- Ability to support a custom graph execution engine
- Expected query-time performance
- Developer ergonomics
- Suitability for testing, benchmarking, and future evolution

## A Note on Query-Time Performance

Query-time speed needs to be split into two different categories:

- **Tabular query speed**
  - scans
  - filters
  - projections
  - sorts
  - joins
  - aggregations
- **Graph query speed**
  - adjacency expansion
  - multi-hop traversal
  - frontier evolution
  - graph-shaped join and binding propagation

This distinction matters because a backend can be excellent at tabular execution and still not be the best fit
for graph traversal execution.

For GraphArrow, both dimensions matter, but they matter differently:

- tabular speed is important for `WHERE`, `RETURN`, `ORDER BY`, filtering, admin tasks, and some join-heavy queries
- graph speed is critical for multi-hop traversal and graph-native workloads

## Summary Recommendation

### Recommended direction

- Use `pyarrow` as the core data access and execution substrate
- Optionally use `duckdb` as a sidecar tool for debugging, verification, benchmarks, and analytics
- Keep `polars` as a possible experimentation layer, not as the core storage/runtime dependency
- Do not use `pandas` as the main backend

### Why

GraphArrow is not "just a query layer on Parquet".
It is a graph engine with:

- custom storage layout
- snapshot manifests
- explicit graph traversal semantics
- custom commit and recovery rules
- future adjacency-aware operators

That pushes the architecture toward a library that gives us:

- low-level control
- strong Parquet and Arrow integration
- clean interoperability with batch execution

`pyarrow` is the best fit for that role.

## Option 1: PyArrow

### What it is good at

`pyarrow` is the most direct fit for:

- Parquet I/O
- Arrow-native memory representation
- dataset scanning
- projection pushdown
- predicate pushdown
- batch-oriented processing

It also aligns naturally with the design we already chose:

- Arrow-friendly execution direction
- Parquet as canonical storage
- custom graph data structures and derived adjacency

### Pros

- Best native fit with Parquet and Arrow
- Excellent foundation for custom execution operators
- Clean control over schemas, arrays, batches, and tables
- Strong support for dataset-style reads and file-level pruning
- Good fit for S3/object-storage oriented layouts
- No need to embed a second query engine inside GraphArrow
- Makes future Arrow-native query execution plausible

### Cons

- Lower-level than the other options
- More work to implement filtering, joins, and graph operators ourselves
- Less ergonomic than SQL or DataFrame APIs for rapid prototyping
- More engineering effort up front

### Architectural impact

Using `pyarrow` as the core means:

- GraphArrow owns execution semantics
- we build graph operators explicitly
- we can progressively map parts of Cypher to Arrow-style operators

This is the cleanest long-term architecture if GraphArrow wants to be a real engine.

### Where PyArrow could win

`pyarrow` is most likely to win when:

- we want the cleanest direct path from Parquet files to execution batches
- we care about controlling schemas, arrays, record batches, and file scans precisely
- we want to build graph-aware operators ourselves instead of delegating execution shape to another engine
- we want adjacency-aware execution to stay close to the storage layout
- we want the least impedance mismatch between storage, memory model, and executor

In short:

- `pyarrow` may not always be the shortest path to a fast tabular join
- but it is often the best path to a coherent engine architecture

## Option 2: Pandas

### What it is good at

`pandas` is very convenient for:

- debugging
- ad hoc local inspection
- small examples
- developer familiarity

### Pros

- Extremely familiar to most Python users
- Great for debugging and small-scale manipulation
- Good ecosystem and easy adoption
- Convenient in notebooks and quick experiments

### Cons

- Not a strong backend for a Parquet-first graph engine
- More row-oriented and object-heavy than Arrow-native execution
- Less suitable for large-scale or lazy Parquet scans
- Worse fit for S3/object-storage-native access patterns
- Easier to accidentally leave the Arrow model and materialize too much in memory
- Not a good base for a custom graph runtime

### Architectural impact

If GraphArrow used `pandas` as the core backend, we would likely:

- lose Arrow-native direction
- build a Python-object-heavy execution path
- make future traversal performance harder to improve

### Verdict

Useful for development tooling or convenience conversion.
Not recommended as the main backend.

### Where Pandas could win

`pandas` could still win in a few narrow cases:

- tiny datasets where developer speed matters more than engine purity
- notebook exploration and debugging
- tests where readability matters more than performance
- quick one-off inspection flows where everyone already thinks in DataFrames

In practice, these are convenience scenarios, not core runtime scenarios.

## Option 3: Polars

### What it is good at

`polars` gives a much better developer experience than raw `pyarrow` while still feeling modern and efficient.
It is particularly attractive for:

- fast tabular transforms
- lazy query pipelines
- pushdown-friendly scans
- rapid prototyping of analytical flows

### Pros

- Much more ergonomic than raw Arrow
- Good performance
- Lazy execution model is attractive
- Better than `pandas` for large tabular work
- Often productive very quickly

### Cons

- It becomes an additional execution model sitting between GraphArrow and Arrow
- Less direct control than raw `pyarrow`
- Graph traversal semantics are still ours to implement anyway
- Risk of building the engine around a DataFrame abstraction that does not match graph execution well
- If we later need precise batch and memory control, we may end up bypassing Polars in hot paths

### Architectural impact

Using `polars` as the main backend could be tempting early because it is productive.
But it risks creating a mismatch:

- tabular operations become very easy
- graph-specific operators still need custom logic
- the engine can become split between "Polars for table work" and "custom Python for graph work"

That may be acceptable for tooling, but less ideal for the core runtime.

### Verdict

Good candidate for experimentation and maybe some higher-level tooling.
Not the best choice as the fundamental engine substrate.

### Where Polars could win

`polars` could realistically win in these areas:

- tabular joins between node and edge datasets
- filter + projection pipelines
- sorting and aggregation
- lazy query planning over Parquet scans
- fast analytical queries that are more relational than graph-traversal-heavy

It is especially attractive if an operator looks like:

- read a few Parquet files
- filter them
- join them
- project a result

For example, `polars` may be very compelling for:

- validating constraints over tabular snapshots
- building admin/reporting queries
- evaluating some `MATCH + WHERE + RETURN` cases that are close to relational joins

The risk is not that `polars` is weak.
The risk is that it may be excellent at the tabular half of the problem while leaving the graph half outside its natural model.

## Option 4: DuckDB

### What it is good at

`duckdb` is excellent for:

- querying Parquet directly
- SQL over local or remote files
- joins, filters, aggregations, sorting
- analytics and debugging
- quick verification against stored data

### Pros

- Very strong Parquet and SQL experience
- Great performance for many analytical workloads
- Excellent for validation, admin commands, benchmarks, and exploratory analysis
- Good option for sidecar tools around the storage layer
- Can be very useful when checking data correctness during development

### Cons

- It is a query engine of its own, not just a storage library
- Embedding it deeply would make GraphArrow depend on a second execution engine
- Graph traversal semantics do not map naturally to DuckDB as a core graph runtime
- Harder to express "this is our graph operator pipeline" if SQL becomes the real engine under the hood
- Risks turning GraphArrow into a graph-flavored frontend over DuckDB instead of a graph engine

### Architectural impact

If DuckDB becomes central, the architecture may drift toward:

- storage in Parquet
- SQL execution in DuckDB
- graph semantics partly simulated on top

That can be powerful, but it is a different product direction.

### Verdict

Very attractive as a supporting tool.
Not recommended as the core execution substrate if GraphArrow wants to own graph semantics.

### Where DuckDB could win

`duckdb` could win very clearly in:

- SQL-style analytical queries over Parquet
- heavy join workloads
- ad hoc data validation and consistency checks
- quick exploratory queries during development
- benchmark baselines for scans, joins, filters, and aggregations

It may also be the best tool for answering questions like:

- "How fast can a mature columnar engine join these node and edge tables?"
- "What is the upper bound for this analytical workload if we treat it as relational?"

That makes DuckDB extremely valuable as a benchmark and diagnostics companion, even if it is not the right engine core.

## Comparison Table

| Option | Parquet/S3 fit | Low-level control | Tabular query speed | Graph query fit | Ergonomics | Best role |
| --- | --- | --- | --- | --- | --- | --- |
| `pyarrow` | Excellent | Excellent | Good to very good | Excellent | Medium | Core substrate |
| `pandas` | Weak to medium | Low | Weak to medium | Weak | Excellent | Debugging and convenience |
| `polars` | Good | Medium | Very good to excellent | Medium | Very good | Experimentation and tabular helpers |
| `duckdb` | Excellent | Medium | Excellent | Medium to weak as core | Very good | Sidecar analytics and verification |

## Performance-Oriented Read

If the main question is:

> "Which one is likely to be fastest at query time?"

the honest answer is:

- for **tabular query execution**, `duckdb` and `polars` are often the strongest candidates
- for **a custom graph engine that must own traversal semantics**, `pyarrow` is still the safest foundation

So the trade-off is:

- `duckdb` / `polars` may give faster wins on joins and analytical query pieces
- `pyarrow` gives better control for the graph-specific hot path we expect to matter long term

In other words:

- if GraphArrow were mainly a relational query layer over Parquet, `duckdb` or `polars` would look even stronger
- because GraphArrow wants to be graph-native, query-time speed must be balanced against graph execution control

## Recommended Decision for GraphArrow

At this stage, the recommended choice is:

- **Core backend:** `pyarrow`
- **Primary storage/runtime model:** Arrow-native batches over Parquet datasets
- **Possible support tool:** `duckdb`
- **Optional experimentation:** `polars`
- **Non-core convenience only:** `pandas`

This gives us:

- the cleanest path toward an Arrow-native executor
- the best compatibility with our storage design
- freedom to implement graph-specific operators without being boxed into a tabular engine

## Practical Guidance

### Use PyArrow for

- canonical Parquet read/write
- dataset scanning
- projection and filtering building blocks
- batch-level execution internals
- interoperability between storage and the future query engine

### Use DuckDB for

- debugging stored Parquet files
- offline analysis
- consistency checks
- benchmarks against alternative execution approaches

### Use Polars only if

- we want to prototype a specific analytical path quickly
- we keep it outside the critical core execution path

### Use Pandas only for

- developer convenience
- testing helpers
- notebook inspection

## Future Re-evaluation Triggers

We should revisit this decision if any of the following becomes true:

- most of the real workload turns out to be analytical rather than graph traversal
- we decide GraphArrow should become more like a graph query layer over external engines
- Arrow-native custom execution proves significantly too expensive to maintain
- DuckDB or Polars clearly outperform our needs in a way that changes product direction

## Final Take

If GraphArrow wants to remain:

- a graph database engine
- Parquet-backed
- S3-friendly
- snapshot-based
- traversal-oriented

then `pyarrow` is the right default foundation.

If GraphArrow wanted instead to become:

- a graph facade over Parquet analytics

then `duckdb` or `polars` would become much more attractive.

Right now, the architecture we have been defining still points clearly toward `pyarrow` as the core choice.
