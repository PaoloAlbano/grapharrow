# Cypher Correctness Status

GraphArrow implements an experimental Cypher subset. Accepted syntax does not
guarantee correct execution. This page distinguishes corrected defects from
remaining gaps; it is not a claim of complete openCypher compatibility.

See [Cypher V1](cypher-v1.md) for implemented query families and
[Transactions and Sessions](transactions.md) for publication and transaction limits.

## Corrections after 0.1.0

The following defects were reproduced through the embedded Python API on
2026-11-08/09 and corrected in the source tree after the 0.1.0 release. Existing
0.1.0 artifacts do not contain these corrections.

### Relationship creation direction

```cypher
CREATE (:A)<-[:R]-(:B)
```

This now stores a relationship from B to A. Supported `MATCH ... CREATE` and
row-driven creation paths preserve the same direction. Undirected `CREATE`
patterns are rejected before graph publication. Related relationship `MERGE`
paths preserve incoming direction and reuse a reversed existing relationship
when the merge pattern is undirected.

### Self-loop match multiplicity

```cypher
CREATE (n:A)-[:LOOP]->(n)
```

An undirected match emits this self-loop once per input binding, including in
counts. Non-loop relationships still have two orientations in an undirected
match. Parallel relationships and duplicate input rows keep their multiplicity;
the correction does not apply a global `DISTINCT`.

### Null-valued entity properties

```cypher
MATCH (n:A)
SET n.property1 = null
RETURN n
```

The property is removed from the canonical record and remains absent after
reopening the database. Cypher creation omits top-level null properties, and
supported map updates and `MERGE` actions remove them. A null identity property
in a `MERGE` pattern is rejected rather than silently omitted.

Null values in expressions, lists and nested maps retain their meaning. Native
property APIs keep their existing ability to store null values; this correction
applies to Cypher writes.

### Result windows after writes

```cypher
CREATE (n:N {num: 42})
RETURN n
LIMIT 0
```

The node is created and the result contains zero rows. Standalone `CREATE` and
node `MERGE` use the shared result finalizer for projection, `DISTINCT`,
`ORDER BY`, `SKIP` and `LIMIT`, after performing the write. Upstream `WITH`
windows continue to control which rows reach a subsequent write.

### Binding validation and client errors

The compiler rejects undefined variables in reads and write values, conflicts
between node and relationship bindings, repeated relationship variables in one
match pattern, and known incompatible function or boolean operands. These
checks also apply to empty matches and unselected boolean operands.
`RETURN *` requires a visible named binding and orders columns alphabetically.

Explicit property and label reads on deleted entities raise `QueryError` before
the failing statement publishes a deletion. Capture a needed property with
`WITH` before `DELETE`. Identity and relationship type metadata remain available.
Excessive query nesting produces a query-limit error rather than exposing a
Python recursion exception. This is an implementation limit, not full support
for arbitrarily nested standard expressions.

### Aggregations, WITH and expression values

- `collect(n)` and `collect(r)` retain complete entities, including alongside
  `count` on the same variable. Grouped collections omit nulls. Numeric `min`
  and `max` preserve the selected value's type when inputs mix integers and floats.
- A `WITH` filter can use incoming bindings without exposing them to later
  clauses. After grouping or `DISTINCT`, it can also reference an expression
  that is itself a projected grouping key. Ordering cannot introduce hidden
  values or new aggregates after such a projection.
- String literals decode Cypher escapes and Unicode, including in stored nested
  maps. Negated predicates preserve null values until the final filter.
- Exponentiation follows the grammar's precedence and returns floating-point
  values. Explicit null slice bounds produce null; omitted bounds remain open.
  An exact variable-length binding such as `r*1..1` returns a one-element list.
- Encoded heterogeneous properties are interpreted as values when comparing or
  aggregating them; physical JSON strings are not compared as user strings.

### Fixed-length paths and multi-hop writes

Directed `CREATE` supports multiple relationships in one pattern. Fixed-length
paths can be bound by `MATCH`, `OPTIONAL MATCH`, `CREATE` and `MERGE`.
The complete path is returned in traversal order, including native Bolt
hydration. Multi-hop `MERGE` checks the complete pattern before creating
entities, and conflicts roll back the statement.

Named reads can also return bounded variable-length paths, including
all intermediate nodes and relationships. The bound applies per segment.
An explicit zero-length segment can match a single node.

### Regression checks

[Write correctness tests](../tests/unit/test_cypher_write_correctness.py) cover
stored records, reopening, multiplicity, result windows and transaction behavior.
[Semantic correctness tests](../tests/unit/test_cypher_semantic_correctness.py)
cover binding errors, aggregation values, projection scope, escapes, nulls and
deleted-entity access, including failure without publication.
[Statement pipeline tests](../tests/unit/test_cypher_statement_pipeline.py)
cover error classification, successive mutations and rollback after a
later clause fails.
[Path tests](../tests/unit/test_cypher_paths.py) and
[multi-hop MERGE tests](../tests/unit/test_cypher_multihop_merge.py) cover
ordered paths, null bindings, partial matches and transaction behavior.
[Interface comparisons](../tests/integration/test_interface_comparison.py)
exercise the shared engine through embedded, REST and Bolt entry points,
including Bolt commit/rollback and visibility through MCP and ANA reads.
[Neo4j reference tests](../tests/integration/test_cypher_reference.py) compare
representative writes and their persisted effects against a disposable Neo4j
instance. ANA tools have separate execution paths: read visibility does not
establish identical write semantics for every ANA tool.

## Remaining semantic validation and standard coverage gaps

The engine is not certified as conformant with the
[openCypher M23 TCK](https://github.com/opencypher/openCypher/tree/1.0.0-M23/tck).
Remaining gaps include:

- some errors still have an unclassified detail or differ from the
  expected openCypher M23 classification;
- general procedures, unbounded traversal and updates inside subqueries or
  `UNION` remain outside the supported subset;
- named time zones are limited to years 1 through 9999, and temporal queries
  are subject to the same disconnected-pattern restrictions as other queries;
- unsupported initialization queries can block a TCK scenario during setup;
- nesting and bounded traversal limits restrict some valid standard queries.

The local unit suite and selected differential tests cover narrower workloads.
Their passing counts must not be presented as complete openCypher coverage.

## openCypher M23 audit

The [openCypher M23 workflow](https://github.com/PaoloAlbano/grapharrow/actions/workflows/opencypher.yml)
runs exclusively on `main`, after pushes or on manual dispatch. It
checks an exact revision of the upstream M23 corpus through GraphArrow's
embedded query API and pins the time zone database for reproducible historical
offsets. Each run includes the upstream and GraphArrow commit SHAs,
an outcome for each scenario, and a summary.

The README badge shows the percentage and count of passing scenarios over all
cases selected by M23, excluding cases tagged `@ignore` upstream. Unsupported
procedure fixtures, blocked setups and unverified errors stay in the
denominator. A red badge indicates an audit failure, an unexpected result
or a regression against the previous successful run. The badge and baseline
are updated on the `badges` branch.
