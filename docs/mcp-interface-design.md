# Cypher MCP Interface

GraphArrow provides a transport-independent Cypher-oriented tool interface.
It exposes existing query and catalog use cases without implementing a complete
MCP network transport. The implementation is in
[mcp_cypher.py](../src/grapharrow/interfaces/mcp_cypher.py);
[mcp.py](../src/grapharrow/interfaces/mcp.py) retains compatible public imports.

This is the currently implemented interface. It does not provide the ANA tool
catalog, ANA workflow sessions or an ANA-compatible HTTP RPC server.

## Python entry point

```python
from grapharrow.interfaces.mcp import create_mcp_interface

graph = create_mcp_interface("/tmp/grapharrow-db")
definitions = graph.list_tools()
result = graph.call_tool(
    "query",
    {"query": "MATCH (n:Person) RETURN n.name", "max_rows": 20},
)
```

`create_mcp_cypher_interface` builds the same Cypher-oriented interface.
`list_tools()` returns definitions containing each tool's name, description
and input schema. `call_tool(name, arguments)` dispatches a tool invocation.

## Tools

| Name | Arguments | Behavior |
| --- | --- | --- |
| `query` | Required `query`; optional `params`, `max_rows`, `max_hops` | Execute read-only Cypher, check hop limits and truncate returned rows. |
| `write_cypher` | Required `query`; optional `params` | Execute write-capable Cypher through the same query use case. |
| `get_schema` | None | Return catalog version, labels, relationship types and property definitions. |
| `get_constraints` | None | Return catalog version and logical constraint definitions. |
| `get_indexes` | None | Return catalog/snapshot versions and derived dataset file metadata. |

### Query results and limits

Both query tools return `columns`, `rows`, `row_count`, `returned_row_count`,
`truncated`, `summary` and `warnings`. Counts distinguish the full query result
from the rows returned by the interface.

For `query`, `max_rows` defaults to 100 and accepts values from 1 to 1000.
`max_hops` defaults to 5 and accepts values from 1 to 5. It limits the
maximum length of each parsed pattern chain, including the sum of its
variable-length segments' upper bounds. A detected write statement raises
`TransactionError`. Invalid limits or excessive hops raise `QueryError`.

Row truncation happens after query execution. The hop check runs before
execution. The number of candidate paths also depends on the graph topology.

`write_cypher` does not apply the read tool's row/hop limits. It supports the
same [Cypher subset](cypher-v1.md) as the underlying query use case.

### Catalog and derived datasets

`get_schema` includes property ownership, name, type, nullability and deprecation.
`get_constraints` includes constraint kind, owner, property and endpoint labels.

`get_indexes` reports these manifest collections:

- `node_labels`;
- `node_properties`;
- `edge_properties`;
- `adjacency_by_source`;
- `adjacency_by_dest`.

File entries include paths, row counts, checksums and available key bounds.
This reports the derived structures recorded in the snapshot; it does not mean
that general physical secondary indexes or full-text search are implemented.

## Errors and access control

The Python interface raises GraphArrow exceptions from validation and execution.
Unknown tool names raise `QueryError`. A transport adapter is responsible for
mapping those exceptions to its own protocol error format.

The current interface has no `readonly`/`readwrite` profile selection or
per-client authentication layer. `write_cypher` is a separate explicit tool,
but callers integrating this interface must decide which tools to expose and
authorize. The read-only check on `query` does not authorize other tool calls.

See [MCP tests](../tests/unit/test_mcp.py) for executable examples of discovery,
dispatch, query guards and output metadata.
