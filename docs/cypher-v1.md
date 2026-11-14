# Cypher V1 in GraphArrow

GraphArrow implements a subset of Cypher over snapshot-based Arrow/Parquet
storage. This guide describes the current execution model, supported query
families and limitations. Parsing the full openCypher grammar does not imply
that every valid Cypher query can be executed.
The [correctness status](known-cypher-issues.md) describes recent fixes and
remaining validation and execution gaps.

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

The compiler checks GraphArrow's supported subset after syntax parsing, including
binding scope, node/relationship conflicts and known invalid operand types.
The [known issues](known-cypher-issues.md) describe remaining gaps. The
[V1 planner](planner-v1.md) covers selected read shapes; other supported shapes
use the direct executor. Query support is broader than planner coverage.
Composable read queries use explicit stages in
[cypher_pipeline.py](../src/grapharrow/use_cases/cypher_pipeline.py) and typed
expressions in [cypher_expression.py](../src/grapharrow/use_cases/cypher_expression.py).

## Query errors

Cypher errors carry a machine-readable `code`, `category`, `phase` and
`detail`, alongside a human-readable message. The phase is `compile` for
parsing and semantic checks or `runtime` for execution. Syntax errors from
the parser include a one-based `line` and a zero-based `column`.
For example, an unknown binding has category `SyntaxError`, phase `compile`
and detail `UndefinedVariable`.

| Interface | Error metadata |
| --- | --- |
| Embedded and MCP Cypher | `QueryError` attributes and `metadata()` |
| REST `POST /v1/query` | HTTP 400 with `error.details`. The envelope retains `error.code = "QUERYERROR"` and `error.message`. |
| Bolt | `FAILURE.code` and `FAILURE.message` with the same fields under `grapharrow_error`. Driver access to extra metadata varies. |

The classifications cover supported error cases. Some failures use
`GraphArrow.QueryError` with detail `Unclassified`. Unsupported features can
use `GraphArrow.UnsupportedFeature`. Clients can use the machine-readable
fields to branch on a classified error and show the message to the caller.

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

`range(start, end[, step])` returns a list of signed 64-bit integers. The
default step is one. A negative step produces a descending list, and the end
value is included when the progression reaches it. If the step points away
from the end, the result is an empty list. Null and noninteger arguments,
as well as a zero step, raise query errors. GraphArrow supports
up to 1,048,576 values per `range()` call.

`head(list)` and `last(list)` return the first and last values. Both return
null for an empty list. `tail(list)` returns the list without its first
value. `properties(value)` returns the property map of a node or relationship
or the entries of an input map. These functions propagate null.

Scalar conversions accept the following inputs.

| Function | Supported inputs |
| --- | --- |
| `toInteger()` | Integers, floats, booleans and numeric strings |
| `toFloat()` | Integers, floats and numeric strings |
| `toBoolean()` | Booleans, integers and case-insensitive `"true"` or `"false"` strings |
| `toString()` | Strings, integers, floats, booleans and temporal values |

All four functions return null for a null input. An unparseable string
returns null when converting to a number or boolean. Unsupported input
types raise a query error. Integer conversion truncates fractional values
toward zero. Floating-point arguments beyond the signed 64-bit range
saturate at the nearest endpoint, while out-of-range numeric strings raise
an error. For boolean conversion, zero maps to false and any other integer
maps to true. `sign()` returns `-1`, `0` or `1` for numeric inputs and
returns null for null.

`toFloat()` also accepts the strings `"NaN"` and `"Infinity"`. Non-finite
floats are represented as null in REST JSON results. Use `toString()` when
the result needs to preserve their textual spelling over REST.

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
Exponentiation returns a floating-point value. Integer literals must fit a signed
64-bit value. String literals decode Cypher escapes and Unicode. A list slice
with an explicit null bound returns null; an omitted bound leaves that end open.

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

### Temporal values

`date()`, `localtime()`, `time()`, `localdatetime()` and `datetime()` accept
ISO strings, maps of fields, and compatible temporal values. Constructors
support calendar dates, ISO week dates, ordinal dates and quarter fields.
Zoned values support numeric UTC offsets, and zoned date times also accept
IANA names such as `Europe/Rome`. Use property access for components such as
`d.dayOfWeek`, `t.nanosecond` and `dt.epochMillis`.

- Use `datetime.fromEpoch(seconds, nanoseconds)` or
  `datetime.fromEpochMillis(milliseconds)` to construct UTC instants.
- Each instant function has a `.truncate(unit, input[, fields])` variant.
- Zero-argument instant functions use the statement clock. The
  `.transaction()`, `.statement()` and `.realtime()` variants accept an
  optional time zone. With no explicit time zone, the default is UTC.
- `duration()` accepts ISO strings and maps of duration fields.
  `duration.between()`, `duration.inMonths()`, `duration.inDays()` and
  `duration.inSeconds()` calculate differences. Cypher supports addition and
  subtraction with durations, and multiplication and division of a
  duration by a number.

```cypher
WITH datetime('2024-03-30T12:00:00[Europe/Rome]') AS start
RETURN toString(start + duration('P1D')) AS next_day,
       duration.inSeconds(start, start + duration('P1D')).seconds AS elapsed
```

Node and relationship properties preserve temporal types and nanosecond
precision through storage, including in lists of temporal values. Bolt
returns native temporal structures and accepts them as parameters. The
embedded query API, REST and MCP expose ISO strings, as do ANA property
results. Use a Cypher constructor such as `date($value)` to interpret an ISO
string parameter as a temporal value. Named time zones use the system IANA
database and support years 1 through 9999. Extended years are available
for local and fixed-offset values within the Cypher range and can exceed
the representable range of some client drivers.

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
even if they are not projected. After aggregation or `DISTINCT`, the predicate
can reference projected variables and expressions that are themselves projected
grouping keys, such as `WITH DISTINCT a.name AS name WHERE a.name = 'B'`.
Other incoming values are unavailable. Subsequent clauses see only the projected
scope. Within a `WITH`, ordering and
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

Fixed-hop and bounded variable-length traversal enforce relationship
uniqueness within the supported pattern execution. Typed traversal can use
derived adjacency datasets for outgoing and incoming access.

A fixed-length pattern can bind a path in `MATCH`, `OPTIONAL MATCH`, `CREATE`
or `MERGE`:

```cypher
MATCH p = (:Person)-[:KNOWS]->(:Person)<-[:KNOWS]-(:Person)
RETURN length(p) AS hops, nodes(p) AS ns, relationships(p) AS rs
```

`length(p)` counts relationships. `nodes(p)` and `relationships(p)` follow
traversal order and retain each relationship's stored source and target.
Returning the path itself gives a mapping with `nodes` and `relationships`
in embedded, REST and MCP, and a native Path over Bolt. An unmatched
`OPTIONAL MATCH` produces a null path. `DETACH DELETE p` deletes the path
entities and their incident relationships.

```cypher
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.name = $name
RETURN DISTINCT b.name
```

A named path in `MATCH` or `OPTIONAL MATCH` can include bounded
variable-length segments. Each segment can bind its relationships to a list.
The path retains all intermediate nodes and edges in traversal order.

```cypher
MATCH p = (a:Person)-[rs:KNOWS*0..3]-(b:Person)
WHERE a.name = $name
RETURN p, rs, length(p) AS hops
```

Named paths and relationship-list bindings support exact lengths, closed
ranges, and ranges with an omitted lower bound such as `*..3`. The lower
bound defaults to one. An explicit zero-hop segment binds both endpoints
to the same node and produces an empty relationship list. Each variable-length
segment has an upper limit of five hops. Ranges without an upper bound
remain unsupported.

## Write queries

Writing statements can combine successive `CREATE`, `MERGE`, `SET`,
`REMOVE`, `DELETE` and `DETACH DELETE` clauses with `MATCH`,
`OPTIONAL MATCH`, `WITH` and `UNWIND`. Later clauses see the changes made
earlier in the statement. `SET` supports individual properties, property-map
replacement with `=`, map updates with `+=` and adding node labels.
Use `REMOVE` to drop properties or labels.

```cypher
UNWIND [1, 2, 3] AS x
CREATE (n:Example {x: x})
WITH n
SET n:Ready, n.value = n.x * 10
REMOVE n.x
RETURN n.value AS value ORDER BY value
```

Directed `CREATE` patterns can chain multiple relationships while preserving
their written directions. For example, `(a)-[:R]->(b)<-[:S]-(c)` creates
relationships from `a` to `b` and from `c` to `b`. An undirected
`CREATE` relationship is rejected before publication.

Cypher entity properties cannot contain top-level null values. `CREATE` omits
them, and `SET n.p = null` or `SET n += {p: null}` removes `p` from the stored
record. The same behavior applies to relationships and supported `MERGE` actions.
Null identity properties in a `MERGE` pattern are rejected. Null values in
expressions, lists and nested maps keep their existing meaning; the native
property API retains its separate null-value contract.

`RETURN DISTINCT`, `ORDER BY`, `SKIP` and `LIMIT` apply to the result after a
supported write has executed. For example, `CREATE (n:N) RETURN n LIMIT 0`
creates the node and returns no rows. A window on an upstream `WITH` instead
controls which rows reach the write. `SKIP` and `LIMIT` require nonnegative
integer values, including when supplied as parameters.

Explicit property or label access on an entity deleted by the statement is an
error. Use `WITH` to capture values before deleting an entity.

```cypher
MATCH (n:Person {name: $name})
WITH n, n.name AS name
DETACH DELETE n
RETURN name
```

`id(n)`, `id(r)` and `type(r)` retain their metadata meaning after deletion.

`MERGE` supports nodes and fixed-length chains with `ON CREATE SET` and
`ON MATCH SET`. It searches for the entire pattern. If no complete match
exists, it creates the pattern using entities already bound by earlier
clauses. Each input row can match changes made by previous rows of the same
`MERGE`. Undirected relationships can match either stored direction.

Updating graph statements stage their changes in memory. Successful
autocommit statements publish at most one snapshot, and an error during
execution or result evaluation discards the staged changes. See
[Transactions and Sessions](transactions.md) for explicit Bolt transactions
and storage publication behavior.

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
- unbounded traversal and unrestricted disconnected pattern matching;
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
