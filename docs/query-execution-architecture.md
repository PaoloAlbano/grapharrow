# Query Execution Architecture

GraphArrow uses a grammar-based Cypher frontend and an execution engine built
on Arrow tables and Parquet-backed snapshots. The same query use case serves
the embedded API, REST, Bolt and the transport-independent MCP interface.

## Module boundaries

```text
embedded / REST / Bolt / MCP
  -> CypherQueryUseCase
  -> openCypher parser and compiled query model
  -> supported logical plan or direct execution
  -> graph state and tabular operators
  -> repositories and storage adapters
  -> Parquet datasets, catalog and snapshot manifests
```

Interfaces validate and serialize their own request/response formats. Query
semantics live in [cypher.py](../src/grapharrow/use_cases/cypher.py), with graph
state and transaction operations in [graph.py](../src/grapharrow/use_cases/graph.py).

## Parsing and compilation

The official [openCypher grammar](../vendor/opencypher/Cypher.g4) is vendored
and parsed through ANTLR. The runtime adapter is
[adapters/opencypher.py](../src/grapharrow/adapters/opencypher.py).

Compilation converts the parse tree into typed internal query dataclasses and
pattern/expression structures. The compiler enforces the executable subset
after parsing. Valid openCypher syntax can therefore still raise `QueryError`
when it uses an unsupported GraphArrow feature.

Supported query families include reads, selected `WITH`/`UNWIND` pipelines,
optional patterns and graph writes. Some metadata/discovery statements have
dedicated compatibility handling. See [Cypher V1](cypher-v1.md) for the current
scope and restrictions.

## Planning and execution

The [V1 planner](planner-v1.md) lowers supported read shapes to explicit scan,
expand, filter, projection, aggregation, distinct, sort and slice operators.
Other supported shapes use direct execution or specialized paths. Plans are
evaluated recursively; an explicit plan is not a guarantee that every step
avoids Python row processing.

Read operations use Arrow binding tables. Columns represent node/relationship
variables and requested fields. Pattern joins propagate bindings, predicates
produce boolean masks, and projections shape intermediate and final tables.
[TabularAdapter](../src/grapharrow/adapters/tabular.py) provides reusable
Arrow operations.

Fixed-hop typed traversal can use outgoing or incoming adjacency datasets.
Selective fixed-hop expansions can also use the graph service's shared
canonical-position index before joins. ANA and Cypher reuse the same bounded
application cache for immutable edge files; transaction overlays keep private
indexes. See [Shared Snapshot Adjacency](benchmarks/shared-adjacency.md) for
execution scope, resource accounting and measurements.
Anonymous bounded variable-length traversal maintains a frontier and relationship
uniqueness; selected distinct-endpoint queries have a specialized execution path.
General path objects and unrestricted variable-length planning are not implemented.

Supported grouped and scalar aggregates include count, sum, average, minimum,
maximum and collection. Some paths use Python grouping/materialization.
Optional matching and `UNWIND` also retain row-oriented paths.

## Storage boundary

Repositories expose snapshot data through canonical and derived datasets:

- canonical node and edge tables;
- label-membership tables;
- node and edge property projections;
- adjacency by source and by destination;
- catalog and immutable snapshot manifests, selected by `current_snapshot.json`.

Canonical properties use JSON payloads; derived property datasets expose typed
columns for filtering and projection. Property loaders can request selected
Parquet columns and eligible scalar predicates. The Parquet adapter applies
schema-compatible predicates to the dataset scanner, while the executor
retains its residual filters. See [Parquet Predicate Pushdown](parquet-predicate-pushdown.md)
for supported shapes, safeguards, and configuration. Canonical fallbacks remain
available when a derived path does not cover the requested fields or shape.

Filesystem handling sits below query semantics. The configured root can use
local storage or supported URI-based object storage. Cloud integration tests
are described in [Real Object Storage Testing](real-object-storage-testing.md).

## Writes and results

Write queries resolve or construct graph records, enforce graph constraints
through the write use cases and publish a new snapshot using staged files and
manifest publication. They share this path with imperative graph APIs.
Read-only sessions reject writes.

`GraphTabularState` supplies table-oriented read state. `GraphState` also
includes materialized records for operations that need them. The public
`QueryResult` exposes column names and Python row dictionaries; transport
serialization happens after execution.

## Public interfaces

| Interface | Implementation | Query access |
| --- | --- | --- |
| Embedded Python | [embedded.py](../src/grapharrow/interfaces/embedded.py) | Database/session API |
| REST | [rest.py](../src/grapharrow/interfaces/rest.py) | `POST /v1/query` by default |
| Bolt | [bolt.py](../src/grapharrow/interfaces/bolt.py) | Driver statements and supported transaction messages |
| MCP interface | [mcp_cypher.py](../src/grapharrow/interfaces/mcp_cypher.py) | Cypher tools and catalog inspection |

Bolt buffers query results and emits records through its supported protocol
surface; it is not a complete implementation of Neo4j's server features.
The [MCP interface](mcp-interface-design.md) is transport-independent and does
not provide a complete MCP network server or an ANA-compatible HTTP surface.

## Validation

[Cypher tests](../tests/unit/test_cypher.py) exercise compiler, planner, result
semantics and rejection cases. Interface and storage integration tests verify
their respective boundaries. [Benchmark reports](benchmarks/) preserve
measurements for specific workloads; performance depends on the chosen path,
graph shape and storage environment.
