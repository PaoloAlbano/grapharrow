# Cypher V1 in GraphArrow

GraphArrow implements a subset of Cypher over snapshot-based Arrow/Parquet
storage. This guide describes the current execution model, supported query
families and limitations. Parsing the full openCypher grammar does not imply
that every valid Cypher query can be executed.
The [known issues](known-cypher-issues.md) describe confirmed correctness defects
within otherwise accepted query shapes.

## Query execution

```text
query string and parameters
  -> vendored openCypher grammar with GraphArrow read extensions through ANTLR
  -> typed compiled query
  -> V1 logical plan, direct execution, or composable read stages
  -> Arrow-backed operations and Python fallbacks
  -> QueryResult
```

The entry point is `CypherQueryUseCase.execute` in
[cypher.py](../src/grapharrow/use_cases/cypher.py). Compilation produces query
dataclasses such as `CompiledMatchQuery`, `CompiledOptionalMatchQuery`,
`CompiledMatchWithQuery`, `CompiledUnwindQuery`, `CompiledCreateQuery` and the
supported write-query variants.

The compiler checks GraphArrow's supported subset after syntax parsing.
Unsupported query shapes usually raise `QueryError`; semantic validation is
incomplete and some invalid queries are accepted or expose internal exceptions.
See the [known issues](known-cypher-issues.md). The
[V1 planner](planner-v1.md) covers selected read shapes; other supported shapes
use the direct executor. Query support is broader than planner coverage.
Composable read queries use explicit stages in
[cypher_pipeline.py](../src/grapharrow/use_cases/cypher_pipeline.py) and typed
expressions in [cypher_expression.py](../src/grapharrow/use_cases/cypher_expression.py).

## Read queries

Supported query families include:

- `MATCH`, connected multiple pattern parts and selected successive `MATCH` clauses;
- `OPTIONAL MATCH`, including selected `MATCH ... OPTIONAL MATCH` shapes;
- `WHERE`, `RETURN`, aliases, `DISTINCT`, `ORDER BY`, `SKIP` and `LIMIT`;
- multiple read `WITH` boundaries for projection, grouping and subsequent clauses,
  including `WITH *`;
- `UNWIND` in supported read and data-loader shapes;
- node labels, inline property predicates and fixed-hop relationship variables;
- typed outgoing and incoming traversal, relationship-type alternatives and
  supported bounded variable-length patterns.
- `UNION`, `UNION ALL`, read `CALL { ... }` subqueries and query-form `EXISTS { ... }`.

For example:

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.name = $name
RETURN DISTINCT b.name AS name
ORDER BY name
LIMIT 20
```

Disconnected patterns are restricted: selected bounded node lookups are
supported, while unrestricted implicit Cartesian products are not.

### Expressions and aggregation

The implemented expression subset includes comparisons, boolean composition,
`IN`, null checks, property existence checks, `CONTAINS`, `STARTS WITH`,
`ENDS WITH`, `id`, `labels`, `type`, `keys`, `toLower`, `toUpper`, `size`, `abs`
and arithmetic expressions. Read projections can compose these with searched
and simple `CASE`, `coalesce`, lists, maps, indexing, slicing, list comprehensions
and `all`/`any`/`none`/`single` predicates. Unknown functions and variables are
rejected during compilation, including in an unselected `CASE` branch.

`CASE` evaluates only the selected result; `coalesce` stops at the first non-null
argument. Predicates retain three-valued null semantics. Simple `CASE` compares
using equality, so `CASE null WHEN null ...` does not match; use a searched
`CASE WHEN value IS NULL ...` instead. Integer division truncates toward zero.

Aggregation includes supported uses of `count`, `sum`, `avg`, `min`, `max`
and `collect`, including grouped results and `DISTINCT` operands in composable
reads. `COUNT(DISTINCT *)` and nested aggregates are rejected. An aggregate
projection cannot reference values outside its grouping keys. With empty input,
an aggregate without grouping still produces one row (`count` is zero and
`collect` is empty); a grouped aggregate produces no rows.

```cypher
MATCH (p:Person)
RETURN p.country AS country, count(*) AS total
ORDER BY total DESC
```

### Read pipelines and variable scope

Each `WITH` defines the variables available to subsequent clauses. Expressions
other than a plain variable need an explicit alias in `WITH`. `WITH *` retains
all visible variables. Optional matches preserve the input row and supply nulls
for newly introduced variables when no complete match satisfies their predicate.

```cypher
MATCH (p:Person)
WITH coalesce(p.country, 'unknown') AS country, p.age AS age
WITH country, sum(CASE WHEN age >= 18 THEN 1 ELSE 0 END) AS adults
WHERE adults > 0
WITH country, adults ORDER BY adults DESC LIMIT 10
RETURN country, adults
```

An ordinary, nonaggregate `WITH ... WHERE` may filter using incoming variables
even if they are not projected. After aggregation or `DISTINCT`, only projected
variables are available to that predicate. Within a `WITH`, ordering and
`SKIP`/`LIMIT` apply before its `WHERE` filter. `SKIP` and `LIMIT` require
nonnegative integers and cannot depend on row variables.

### UNION

`UNION` removes duplicate output rows; `UNION ALL` preserves them. Branches
must return the same column names, which may appear in a different order.
The first branch defines output column order. Branches have independent scopes
and their own ordering/windowing. Mixing `UNION` and `UNION ALL` in one query
is rejected. Wrap a union in a read subquery to sort its combined output.

```cypher
CALL () {
  MATCH (p:Person) RETURN p.name AS name
  UNION
  MATCH (c:Company) RETURN c.name AS name
}
RETURN name ORDER BY name
```

### Read subqueries

`CALL (p, q) { ... }` imports named outer variables; `CALL (*)` imports all
visible variables and `CALL ()` imports none. Explicit imports remain available
through inner `WITH` clauses and cannot be shadowed. The legacy form
`CALL { WITH p ... RETURN ... }` accepts a leading importing `WITH` containing
only unaliased outer variables, without filtering, sorting or windowing.
Later `WITH` clauses in that legacy form follow ordinary scope rules.

A returning subquery executes per incoming row. Its exported columns must not
collide with existing outer variables. A subquery yielding no rows removes the
outer row; `OPTIONAL CALL` preserves it with null exports instead.

```cypher
MATCH (p:Person)
CALL (p) {
  MATCH (p)-[:KNOWS]->(friend)
  RETURN count(friend) AS friends
}
RETURN p.name AS name, friends
```

`EXISTS { ... }` correlates outer variables automatically and tests whether its
read body yields any row. Both pattern-only bodies and read clause pipelines
are supported, with or without `RETURN`; nested returning `CALL` subqueries
and unions with matching return columns can be used inside the body.

```cypher
MATCH (p:Person)
WHERE EXISTS {
  MATCH (p)-[:KNOWS]->(friend)
  WITH friend WHERE friend.age >= 18
}
RETURN p.name AS name
```

These subqueries and composable pipelines are read-only. Updating clauses,
unit subqueries without `RETURN`, `CALL ... IN TRANSACTIONS`, arbitrary procedure
calls inside pipelines and unrestricted pattern shapes are not implemented.
Compilation validates every branch before execution; queries do not delegate
execution to Neo4j. The parser uses the unchanged
[openCypher grammar](../vendor/opencypher/Cypher.g4) plus the derived
[GraphArrow grammar](../vendor/opencypher/GraphArrowCypher.g4).

### Traversal semantics

Fixed-hop and anonymous bounded variable-length traversal enforce relationship
uniqueness within the supported pattern execution. Named path objects and
relationship variables on variable-length hops remain unsupported. Typed
traversal can use derived adjacency datasets for outgoing and incoming access.

```cypher
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.name = $name
RETURN DISTINCT b.name
```

Bounded variable-length traversal and specialized distinct-endpoint paths are
implemented separately from the general fixed-hop planner path.

## Write queries

The write subset includes `CREATE`, selected `MATCH ... CREATE` shapes,
`MERGE`, `SET`, property-map updates with `SET +=`, supported `REMOVE`
operations, `DELETE` and `DETACH DELETE`.

`MERGE` supports standalone node patterns, `ON CREATE SET`, `ON MATCH SET`,
selected relationships between matched nodes and supported inline endpoints.
Multi-hop and general ambiguous `MERGE` patterns are not supported.

Selected `UNWIND ... WITH ... CREATE/MERGE/SET` and `MATCH ... WITH` write
shapes support dataset loading and cleanup. These are specific compiler paths,
not general support for arbitrary updating clauses in multi-part queries.

Writes use the same graph transaction and snapshot-publication path as the
imperative APIs. Read-only sessions reject write queries. Syntax parsing
does not replace graph constraints or transaction validation.

## Metadata and index declarations

Selected Neo4j/Memgraph-compatible discovery calls and metadata statements
support graph exploration clients. Examples include schema discovery,
`CALL dbms.components()`, `CALL db.ping()` and the implemented `SHOW` shapes.
This is a compatibility subset, not general procedure execution.

Index DDL supports selected legacy and named `CREATE INDEX`, `CREATE TEXT INDEX`
and `DROP INDEX` forms. Case-sensitive, single-property node `range` and
`lookup` declarations support lazy, bounded in-memory equality indexes for
eligible scalar property projections. Other index kinds remain catalog
metadata; there is no full-text engine. See
[Engine Read and Write Optimizations](engine-performance.md) for scope,
memory settings, and fallback behavior.

## Storage and binding tables

The read engine primarily works with Arrow binding tables. Internal columns
represent variables and fields such as `n.id`, `n.labels`, `n.name`, `r.id`,
`r.type`, `r.source` and `r.target`.

- Pattern matching produces bindings.
- Predicates filter those bindings.
- `WITH` projects or aggregates intermediate results.
- `RETURN` shapes the public result.

[GraphStateUseCase](../src/grapharrow/use_cases/graph.py) exposes
`GraphTabularState` for table-oriented reads and `GraphState` with materialized
records for operations that need them. Canonical node/edge datasets contain
`properties_json`; derived property tables provide typed Arrow columns:

- `derived/node_properties/<Label>`;
- `derived/edge_properties/<REL_TYPE>`;
- `derived/adjacency/by_source/<REL_TYPE>`;
- `derived/adjacency/by_dest/<REL_TYPE>`.

Property projection, predicate pushdown and typed adjacency reduce work where
supported. Composable read stages reuse the Arrow pattern matcher, capture one
graph state for the whole query and materialize intermediate Python rows.
Eligible single-label node scans and scalar projection chains instead use narrow
Parquet reads, reader predicates and Arrow batches; see
[columnar execution](cypher-columnar-execution.md). Other shapes do not receive
all planner pushdowns. Neither path currently streams public query results.
The public `QueryResult` contains `columns` and a tuple of row dictionaries.

## Limitations

The engine is not a complete openCypher implementation. Important limits include:

- updating subqueries, unit subqueries and unrestricted multi-part updates;
- arbitrary procedure calls beyond the implemented discovery subset;
- named paths and general path-valued results;
- general `MERGE` and unrestricted disconnected pattern matching;
- expression/function combinations outside the implemented read subset;
- a planner that covers only selected read shapes.

The [Cypher and planner tests](../tests/unit/test_cypher.py) provide executable
examples of supported behavior and rejection cases. The
[pipeline tests](../tests/unit/test_cypher_pipeline.py) cover composed scopes,
nulls, duplicates and subqueries; the optional
[Neo4j differential suite](../tests/integration/test_cypher_reference.py) checks
results and semantic rejection against a disposable reference database. See
[Query Execution Architecture](query-execution-architecture.md) for the module
boundaries and [benchmark reports](benchmarks/) for recorded measurements.
