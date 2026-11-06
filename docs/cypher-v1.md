# Cypher V1 in GraphArrow

GraphArrow implements a subset of Cypher over snapshot-based Arrow/Parquet
storage. This guide describes the current execution model, supported query
families and limitations. Parsing the full openCypher grammar does not imply
that every valid Cypher query can be executed.

## Query execution

```text
query string and parameters
  -> vendored openCypher grammar through ANTLR
  -> typed compiled query
  -> V1 logical plan for supported shapes, or direct execution
  -> Arrow-backed operations and Python fallbacks
  -> QueryResult
```

The entry point is `CypherQueryUseCase.execute` in
[cypher.py](../src/grapharrow/use_cases/cypher.py). Compilation produces query
dataclasses such as `CompiledMatchQuery`, `CompiledOptionalMatchQuery`,
`CompiledMatchWithQuery`, `CompiledUnwindQuery`, `CompiledCreateQuery` and the
supported write-query variants.

The compiler checks GraphArrow's supported subset after syntax parsing.
Unsupported syntax or semantics raise `QueryError`. The
[V1 planner](planner-v1.md) covers selected read shapes; other supported shapes
use the direct executor. Query support is broader than planner coverage.

## Read queries

Supported query families include:

- `MATCH`, connected multiple pattern parts and selected successive `MATCH` clauses;
- `OPTIONAL MATCH`, including selected `MATCH ... OPTIONAL MATCH` shapes;
- `WHERE`, `RETURN`, aliases, `DISTINCT`, `ORDER BY`, `SKIP` and `LIMIT`;
- a single `WITH` boundary for supported projection, grouping and subsequent clauses;
- `UNWIND` in supported read and data-loader shapes;
- node labels, inline property predicates and fixed-hop relationship variables;
- typed outgoing and incoming traversal, relationship-type alternatives and
  supported bounded variable-length patterns.

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
`ENDS WITH`, `id`, `labels`, `type`, `toLower`, `toUpper`, `size`, `abs` and
selected arithmetic expressions. Pattern-only `EXISTS` and `NOT EXISTS`
checks are supported in selected shapes; they are not general query subqueries.

Aggregation includes supported uses of `count`, `sum`, `avg`, `min`, `max`
and `collect`, including grouped results. `count` and `collect` support
`DISTINCT` operands; other aggregate functions do not support `DISTINCT`.
`COUNT(DISTINCT *)` is rejected. Expression and aggregation support depends
on the query shape; these names do not imply unrestricted expression composition.

```cypher
MATCH (p:Person)
RETURN p.country AS country, count(*) AS total
ORDER BY total DESC
```

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
and `DROP INDEX` forms. These statements maintain catalog declarations;
they do not build general physical secondary indexes or a full-text engine.

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
supported. Some execution and result-shaping paths still use Python rows.
The public `QueryResult` contains `columns` and a tuple of row dictionaries.

## Limitations

The engine is not a complete openCypher implementation. Important limits include:

- `UNION` and general query subqueries;
- arbitrary procedure calls beyond the implemented discovery subset;
- multiple `WITH` boundaries and unrestricted multi-part updates;
- named paths and general path-valued results;
- general `MERGE` and unrestricted disconnected pattern matching;
- `WITH *` and unsupported expression/function combinations;
- a planner that covers only selected read shapes.

The [Cypher and planner tests](../tests/unit/test_cypher.py) provide executable
examples of supported behavior and rejection cases. See
[Query Execution Architecture](query-execution-architecture.md) for the module
boundaries and [benchmark reports](benchmarks/) for recorded measurements.
