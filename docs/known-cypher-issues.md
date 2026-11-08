# Known Cypher Issues

GraphArrow implements an experimental Cypher subset. Accepted syntax does not
guarantee correct execution. The issues below were reproduced through the
embedded Python API on 2026-11-08 and remain unresolved in this checkout.
REST and Bolt use the same Cypher engine. ANA tools have separate execution
paths; these reproductions do not establish the behavior of individual ANA tools.

See [Cypher V1](cypher-v1.md) for the implemented query families and
[Transactions and Sessions](transactions.md) for publication and transaction limits.

## Incoming relationship creation

```cypher
CREATE (:A)<-[:R]-(:B)
```

This should create a relationship from B to A. The observed stored relationship
instead points from A to B. Consequently, the following query returns no rows
instead of one:

```cypher
MATCH (a:A)<-[:R]-(b:B)
RETURN a, b
```

An undirected creation pattern, such as `CREATE (a)-[:R]-(b)`, is also accepted
although openCypher requires an explicit direction for creation.

## Duplicate self-loop matches

```cypher
CREATE (n:A)-[:LOOP]->(n)
```

On this graph, the following undirected match emits the same binding twice
instead of once:

```cypher
MATCH (a)-[r]-(b)
RETURN a, r, b
```

This affects row multiplicity and can change aggregate results. Separate parallel
relationships and other legitimate duplicate rows must retain their multiplicity.

## Null-valued entity properties

Given a node created with `CREATE (:A {property1: 42})`:

```cypher
MATCH (n:A)
SET n.property1 = null
RETURN n
```

Cypher requires the property to be removed. The observed canonical record keeps
`{"property1": null}`, while the returned node omits the property. Result
presentation therefore does not establish that persisted property removal is
correct. The conformance audit also found property side-effect differences in
CREATE, MERGE and map-update cases involving nulls.

Null values remain valid scalar values in expressions, lists and maps; this
issue concerns their interpretation as entity properties during Cypher writes.

## Result windows after writes

```cypher
CREATE (n:N {num: 42})
RETURN n
LIMIT 0
```

The node should be created and the result should contain zero rows. The observed
result contains one row. The corresponding standalone CREATE case with `SKIP 1`
also returns the row instead of an empty result. Read-query window optimizations
do not establish correct write-result windowing.

## Incomplete semantic validation and standard coverage

The engine is not certified as conformant with the
[openCypher M23 TCK](https://github.com/opencypher/openCypher/tree/1.0.0-M23/tck).
Observed gaps include:

- some invalid variable bindings or relationship directions are accepted;
- some queries expose Python or Arrow exceptions instead of a query error;
- generic errors do not provide a verified openCypher error type, phase and detail;
- temporal expressions, general procedures, named paths and unrestricted write
  pipelines are outside the verified executable subset;
- multiple updating clauses and other initialization shapes can prevent TCK
  scenarios from reaching the query under test.

The local unit suite and selected differential tests cover narrower workloads.
Their passing counts must not be presented as complete openCypher coverage.
