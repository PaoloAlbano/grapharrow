# Cross-Interface Result Comparison

GraphArrow keeps embedded Python, REST, Bolt, MCP Cypher and ANA available
together. The comparison suite opens these interfaces on the same temporary
database and checks independently specified expected results.

| Surface | Query/read coverage | Write coverage |
| --- | --- | --- |
| Embedded Python | Cypher through `Database.read_transaction()` | Cypher write session |
| REST | Authenticated `POST /v1/query` | Cypher through the same route |
| Bolt | Real loopback server and the Neo4j Python driver | Autocommit Cypher |
| MCP Cypher | `query` tool on the existing adapter | `write_cypher` tool |
| ANA HTTP | Native tools, short handles and paginated `fetch` | Read-only; observes the published graph |
| CLI | Existing admin/ingest commands; no Cypher query command | Real `bulk-add-nodes` subprocess, checked through all query surfaces |

The MCP comparison exercises the transport-independent adapter, not a separate
MCP network transport. REST and ANA use FastAPI's test client; Bolt uses a real
TCP connection. These tests compare correctness, not transport latency.

## Cases

- Equal names and duplicate application IDs remain separate entities.
- Missing/null properties are returned consistently.
- Cypher parameters and equivalent native ANA filters select the same rows.
- Parallel relationships preserve their multiplicity and relationship properties.
- Grouped counts distinguish edges from distinct target entities.
- Optional incoming relationships retain sources with zero matches.
- Empty scalar counts return zero.
- Writes from Python, REST, Bolt, MCP and CLI ingestion become visible to all
  current Cypher readers and newly created ANA workflows.
- Existing ANA workflows retain their original snapshot, including new calls
  made after a later commit. Existing materialized handles remain stable.
- Two ANA workflows can both use `h1` while resolving different results.

Returned envelopes are normalized to column names and row dictionaries before
comparison. Duplicate rows are retained. Ordered cases compare order exactly;
optional counts are sorted for comparison because that ANA tool has no sorting
argument. Every case also checks explicit expected values, so agreement between
interfaces alone cannot make an incorrect result pass.

## Run and inspect results

```bash
make test-interface-comparison
```

This writes the actual rows, expected values and per-case outcomes to
`/tmp/grapharrow-interface-comparison.json`, plus a standard JUnit report at
`/tmp/grapharrow-interface-comparison.xml`. To choose other paths:

```bash
GRAPHARROW_INTERFACE_COMPARISON_REPORT=/tmp/comparison.json \
  uv run pytest -v tests/integration/test_interface_comparison.py \
  --junitxml=/tmp/comparison.xml
```

The suite requires local loopback sockets and standard development dependencies.
It uses a fresh temporary database, test-only credentials and no cloud account,
Neo4j database service or model endpoint. The ordinary CI test matrix includes
these cases. The separate [ANA reference suite](ana-interface.md#validation)
compares GraphArrow's native tool behavior with an actual Neo4j backend.

See the [executable comparison cases](../tests/integration/test_interface_comparison.py)
and [ANA session contract](ana-interface.md) for details.
