# Planner V1 in GraphArrow

The V1 planner makes selected Cypher read operations explicit as logical plan
nodes. It provides a place for conservative filtering, projection and window
pushdown while retaining the direct executor for other supported queries.
It is not a general cost-based optimizer.

## Execution flow

```text
openCypher parse tree
  -> compiled query dataclass
  -> supported-shape check
  -> logical plan, or direct-executor fallback
  -> Arrow-backed evaluation
  -> QueryResult
```

Plan primitives are defined in
[cypher_plan.py](../src/grapharrow/use_cases/cypher_plan.py). Lowering and
execution live in [cypher.py](../src/grapharrow/use_cases/cypher.py).

The main lowering functions are `_build_match_logical_plan`,
`_build_match_with_logical_plan`, `_build_optional_match_logical_plan` and
`_build_match_optional_logical_plan`. `_build_match_scan_plan` builds the
scan/expansion portion; `_wrap_match_output_plan` adds output operations.

A builder returns `None` when the query is outside its supported shape.
`CypherQueryUseCase._execute_logical_plan_node` evaluates the accepted plan.

## Logical operators

| Operator | Responsibility |
| --- | --- |
| `NodeScanPlan` | Bind an initial node variable using labels, inline properties, requested fields and supported pushed predicates. |
| `ExpandPlan` | Expand a fixed relationship hop from existing bindings, with direction, type alternatives, property requirements and supported edge/end-node predicates. |
| `FilterPlan` | Apply a compiled predicate to intermediate bindings. |
| `AggregateCountPlan` | Compute supported final count aggregates. |
| `OptionalMatchPlan` | Represent an optional branch and its null-preserving bindings. |
| `WithAggregatePlan` | Group and aggregate at a `WITH` boundary. |
| `WithProjectPlan` | Project or rename fields at a nonaggregate `WITH` boundary. |
| `DistinctPlan` | Deduplicate the planned table. |
| `ReturnProjectPlan` | Shape `RETURN` expressions before operations that require the public output shape. |
| `ProjectPlan` | Select binding columns. |
| `SortPlan` | Apply ordering. |
| `SlicePlan` | Apply `SKIP` and `LIMIT`. |

`MatchLogicalPlan` wraps the root and records whether output windowing has been
planned. Plan structure does not guarantee that every underlying operation is
fully vectorized; optional expansion and grouped aggregation still have
Python-based execution paths.

## Supported query shapes

The planner covers selected forms of:

- simple node `MATCH` and linear fixed-hop typed traversal;
- outgoing and incoming expansion;
- `WHERE` predicates on supported bindings;
- `RETURN`, `RETURN DISTINCT`, ordering and result slicing;
- basic standalone and attached `OPTIONAL MATCH`;
- `MATCH ... WITH ... RETURN`, including supported projections,
  nonaggregate `WITH DISTINCT` and count aggregation.

For example, a supported typed path can lower to a node scan, a sequence of
expansions, filters and output operators. Supported single-variable predicates
can be applied as soon as the relevant variable is bound.

General variable-length planning, arbitrary optional patterns, rich aggregation
shapes and write planning remain outside this planner's scope. Such a shape may
still be executable through another engine path; planner coverage and Cypher
support are separate properties.

## Storage access

Plans can take advantage of existing derived datasets:

- node property tables for label/property filtering and projection;
- edge property tables for relationship-property access;
- `adjacency/by_source/<REL_TYPE>` for typed outgoing access;
- `adjacency/by_dest/<REL_TYPE>` for typed incoming access.

Requested fields allow property loaders to select relevant Parquet columns.
Supported property predicates filter columnar data before joins. These access
paths coexist with canonical-table fallbacks; catalog index declarations do
not all imply a physical point-lookup index. Eligible single-property node
`range` and `lookup` declarations can use bounded lazy in-memory equality
indexes; see [Engine Read and Write Optimizations](engine-performance.md).

## Optional branches and output semantics

Optional expansion must preserve the input row when no matching branch exists.
Filter, sorting and slicing placement therefore depends on the query's binding
and null semantics. The planner applies only the supported rewrites.

Internal binding tables and public result tables are distinct. In particular,
deduplication may require `ReturnProjectPlan` before `DistinctPlan` so that
`DISTINCT` operates on the projected answer rather than internal bindings.

## Fallback and limitations

The direct executor remains part of the supported implementation. Fallback lets
the engine execute queries beyond the planner's current subset and retains
paths where an existing specialization is more appropriate.

The planner does not provide a general physical-plan search, cost model,
comprehensive join reordering or full separation of logical and physical plans.
Supported scalar `DISTINCT` paths use Arrow; entity/nested results and some
expression shapes still require fallbacks.

Multiple read `WITH` stages, `CASE`/`coalesce`, unions and read subqueries also
use the [composable read runtime](query-execution-architecture.md#composable-read-stages).
They are supported language features without general lowering into this V1
planner; they retain Python row materialization and the core Arrow matcher.

## Validation and measurements

The [Cypher test module](../tests/unit/test_cypher.py) covers plan construction,
supported pushdown behavior, query results, optional/null behavior and fallback
cases.

The [benchmark reports](benchmarks/) record traversal, count, property-heavy,
optional and `WITH` workloads. Results belong to their recorded graph sizes,
query shapes and configurations; they do not establish that using the planner
always improves latency or memory for every query.
