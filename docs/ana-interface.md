# ANA HTTP Interface

GraphArrow includes a read-only ANA HTTP interface in the same process as its
REST API. It is installed and enabled by default. GraphArrow owns tool dispatch,
authentication, sessions, handles and query execution.

The interface follows the [public ANA v0 tool catalog][ana-catalog] and
[protocol notes][ana-contract]. GraphArrow ships its own schemas and tool
descriptions. Installation and requests require only GraphArrow's dependencies;
they do not fetch the catalog or install the reference package.

[ana-catalog]: https://paoloalbano.github.io/agent-native-graph/#tool-catalog
[ana-contract]: https://github.com/PaoloAlbano/agent-native-graph/blob/main/docs/ana-v0-tool-contract.md

## Start the server

Install GraphArrow and configure the ANA client file:

```bash
uv sync
export ANA_AUTH_FILE=/path/to/private/ana-clients.json
uv run grapharrow-server --path /path/to/database
```

The auth file uses the ANA format:

```json
{
  "clients": [
    {
      "id": "reader",
      "api_key": "REPLACE_WITH_A_RANDOM_KEY_OF_AT_LEAST_32_CHARACTERS",
      "profile": "readonly"
    }
  ]
}
```

Generate a real random key and keep the file outside version control. With no
configured clients, authenticated ANA endpoints return HTTP 503. The existing
optional GraphArrow REST token does not grant ANA access.

ANA authentication protects only the ANA routes. REST write and administration
routes require their own `GRAPHARROW_SERVER_AUTH_TOKEN`; without that setting,
REST remains unauthenticated. Configure both authentication mechanisms before
exposing the combined server outside a trusted local environment.

Use `--no-ana` or `GRAPHARROW_ANA_ENABLED=false` to disable the interface.
`--ana` explicitly enables it. Other settings are:

| Setting | Default | Meaning |
| --- | --- | --- |
| `GRAPHARROW_ANA_SCHEMA_ENTRY` | `overview` | Schema discovery guidance mode |
| `GRAPHARROW_ANA_MAX_SESSIONS` | `128` | Concurrent workflow capacity |
| `GRAPHARROW_ANA_SESSION_IDLE_TIMEOUT_S` | `1800` | Request-driven idle expiry |
| `GRAPHARROW_ANA_MAX_HANDLES_PER_SESSION` | unset | Optional handle count ceiling; unset or `0` disables it |
| `GRAPHARROW_ANA_MAX_ROWS_PER_SESSION` | `100000` | Sum of rows retained across workflow handles |
| `GRAPHARROW_ANA_MAX_BYTES_PER_SESSION` | `67108864` | Accounted snapshot, handle, lineage and page bytes (64 MiB) |
| `GRAPHARROW_ANA_MAX_TOTAL_BYTES` | `536870912` | Shared accounted bytes for the HTTP application (512 MiB) |

Existing REST routes keep their configured prefix, `/v1` by default. ANA uses
the compatible root paths; a root REST prefix is rejected when ANA is enabled.
The embedded API and Bolt remain available independently.

## HTTP contract

| Endpoint | Behavior |
| --- | --- |
| `GET /health` | Public liveness: `{"status": "ok"}` |
| `GET /tools` | Authorized contract metadata, `version: "ana-v0"`; optional `status` filter |
| `GET /tools/specs` | Authorized native LLM schemas, `version: "ana-v0"` |
| `POST /sessions` | HTTP 201 with `session_id` and `idle_timeout_s` |
| `POST /tools/call` | Tool dispatch within the caller's workflow |
| `DELETE /sessions/current` | Release the caller's workflow; HTTP 204 |

Send `Authorization: Bearer <client-key>` on catalog and data endpoints.
Calls and deletion also require `X-ANA-Session: <session-id>`. A session token
does not replace authentication.

Session creation returns the identifier both in the JSON `session_id` field and
the `X-ANA-Session` response header. Successful tool calls and workflow deletion
echo that header and use `Cache-Control: no-store`. Tool execution errors also
echo the header after ownership validation; authentication, unavailable-tool
and unknown/foreign-session errors do not confirm a session. A deletion response
identifies the workflow just closed; its identifier cannot be reused.

Tool calls use the existing ANA envelope:

```json
{"tool": "node_search", "args": {"label": "Device", "value": "sensor", "as": "device"}}
```

The response is `{"tool": "node_search", "result": ...}`. Continue with the
returned handle in subsequent tool arguments; do not guess or transfer handles.
ANA responses are separate from the REST API's `data`/`meta` envelope.

Authentication failures and missing workflow headers return 401. Unknown,
expired or foreign workflows return 404; unavailable or unauthorized tools
return 403; session capacity and resource quotas return 429. Backend creation/cleanup failures return
generic 503 errors. Tool argument/execution failures return 400 with `detail`;
malformed request bodies use FastAPI's 422 validation response.

## Implemented tools

- Schema: `schema_overview`, `schema_search`, `schema_describe_label`,
  `schema_describe_relationship`, `inspect_paths`.
- Handles and diagnostics: `summarize_handle`, `handle_recap`, `repair_empty_result`.
- Native discovery: `node_search`, `node_scan`, `count_nodes`, `entity_resolve`, `value_search`.
- Native graph reads: `expand`, `expand_aggregate`, `optional_expand_count`,
  `optional_count_by_pattern`, `relationship_query`, `multi_hop_query`,
  `pattern_query`, `top_entities_by_property`.
- Constraints and shared roles: `constraint_query`, `same_target_role_intersection`,
  `shared_role_aggregate`.
- Composition: `count_handle`, `filter`, `filter_same_node`, `join_handles`,
  `entity_set_operation`, `combine`, `group_handle`, `project`, `compare`,
  `scalar_compute`, `fetch`.

There are 35 default tools. The discovery mode changes catalog guidance. `/tools/specs?enable_planning_tools=true` additionally
exposes the `draft_tool_plan` and `validate_tool_plan` helpers.
All implemented tools are read-only, even for clients granted `readwrite`.

`constraint_query` applies independent one-hop AND constraints. Use
`same_target_role_intersection` when multiple roles must connect the same pair
of entities. `shared_role_aggregate` finds peers through common intermediate
entities and accepts a previously resolved seed handle.

Pattern filters support node and relationship properties. `distinct=true` on
selected entity properties deduplicates entity bindings, so different nodes
with equal projected scalar values can still produce separate rows. Counts
distinguish parallel edges from distinct target entities. Optional counts retain
source rows with zero matches; source-pattern deduplication happens before the
optional expansion. Pattern query limits apply after grouping and sorting.
The role intersection applies the record limit before list
explosion and local ordering.

For compatible entity-query branches, `count_handle` on a union with an explicit
entity variable and `distinct=true` counts the complete branch membership.
This can exceed the materialized rows when branch queries used a limit.
Fetching the handle still returns only its materialized rows; pagination does
not recover discarded rows. This matches the reference ANA set-count behavior.

## Workflows and results

Each workflow owns independent ANA handles and captures canonical Arrow tables
and property projections from one published GraphArrow snapshot. New commits,
compaction and vacuum do not change that in-memory view. Start a new workflow
to see later graph/schema changes. Internal logical node IDs supply identity;
missing or duplicate application properties named `id` do not merge nodes.
Internal entity references are not serialized into tool JSON.

Handles use short names such as `h1` and `h2`, scoped to their workflow. Two
sessions can both return `h1` for different results. Clients must retain the pair
`(session_id, handle)` and send the matching `X-ANA-Session` header on every call.
A bare handle is neither globally unique nor an authorization token. An unknown
handle in the selected session returns 400; an expired, unknown or foreign
session returns 404. A new session starts its own handle sequence. Internal
entity ownership uses a separate workflow namespace and remains independent of
the short handle name and application properties.

`fetch` follows `ana-fetch-v1`: `rows` contains the full requested page,
`preview` contains at most 20 rows, and `returned_count` equals `len(rows)`.
Use `next_offset` until it is null. `truncated` means more handle rows remain;
`preview_truncated` describes only the preview. Pagination does not undo an
upstream operation's limit.

The session manager serializes calls within a workflow, isolates owners,
expires idle sessions on requests and closes backends on deletion/shutdown.
State is process-local; use one worker. This initial backend loads snapshot
tables and property projections into memory for each session. Handle composition
operates on materialized Python rows.

Quotas count retained state. Snapshot admission includes canonical Arrow tables,
property projections, schema and lookup structures. Handle charges include rows,
metadata, lineage and the most recent fetched page. Shared objects can be charged
again across handles; these are approximate accounting bytes, rather than RSS.
A tool exceeding its quota rolls back its new handles and lineage. Existing
handles and the previous fetched page remain usable. `allow_large` cannot bypass
server quotas. Delete a workflow to release its resources; idle expiry and
shutdown also release its budget.

The handle count ceiling is disabled by default; row and byte quotas remain
active. Set a positive `GRAPHARROW_ANA_MAX_HANDLES_PER_SESSION` to enable an
additional count ceiling. Internal intermediate handles count toward these
quotas, so agents must use returned handle names rather than predict the next
number. Failed operations can leave gaps in the sequence.

Byte admission follows snapshot loading and result construction. It does not
bound temporary allocations, response serialization, CPU time or RSS peaks.
This is a retained-state quota, rather than an operating-system memory sandbox.
The shared budget covers one HTTP application instance; separate processes do
not coordinate it. Embedded callers can pass the same `AnaResourceBudget` to
multiple backends to share a budget. See the
[session resource benchmark](benchmarks/ana-session-resources.md) for measured costs.

GraphArrow performs graph reads over its snapshot and composes results in its
own workflow store. Neo4j and the reference ANA server are only used for the
optional comparison tests.

## Validation

Run contract, snapshot, identity, pagination and HTTP lifecycle tests with:

```bash
uv run pytest tests/unit/test_ana.py tests/unit/test_ana_patterns.py tests/unit/test_ana_roles.py
uv run pytest tests/unit/test_ana_resources.py
```

ANA tests run as part of the standard suite. See
[the interface tests](../tests/unit/test_ana.py),
[the pattern tests](../tests/unit/test_ana_patterns.py) and
[the native adapter](../src/grapharrow/use_cases/ana.py) for executable behavior.

The [reference workflow suite](../tests/integration/test_ana_reference.py)
compares every public tool with ANA on Neo4j, including composed handles,
duplicate entities, parallel edges, aggregates, pagination and lineage. It uses
a small deterministic graph; passing it is bounded compatibility evidence.

To run it, start the reference ANA HTTP service in transaction identity mode
against an empty disposable Neo4j database. Set the test database URI and
credentials, plus the HTTP service URL and an authorized client key:

```bash
export GRAPHARROW_TEST_NEO4J_URI=bolt://localhost:7687
export GRAPHARROW_TEST_NEO4J_USER=neo4j
export GRAPHARROW_TEST_NEO4J_PASSWORD='<disposable-test-password>'
export GRAPHARROW_TEST_ANA_URL=http://localhost:8080
export GRAPHARROW_TEST_ANA_KEY='<disposable-test-client-key>'
uv run pytest tests/integration/test_ana_reference.py
```

The fixture refuses nonempty databases and removes only its own test nodes.
Reference checks are skipped unless both service endpoints are configured.
