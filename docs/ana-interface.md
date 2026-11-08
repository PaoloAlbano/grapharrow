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
| `GRAPHARROW_ANA_READ_CACHE_ENABLED` | `false` | Enable workflow-local candidate caching for `entity_resolve` and `value_search` |
| `GRAPHARROW_ANA_READ_CACHE_MAX_BYTES_PER_SESSION` | `4194304` | Reserved read-cache capacity inside workflow and server byte quotas (4 MiB, minimum 1024 bytes) |
| `GRAPHARROW_ANA_ADJACENCY_INDEX_MAX_BYTES_PER_SESSION` | `8388608` | Ceiling for the computed adjacency-index reservation; `0` disables it (8 MiB) |
| `GRAPHARROW_ADJACENCY_INDEX_MAX_BYTES` | `33554432` | Shared engine adjacency-cache ceiling per application; `0` disables it for ANA and Cypher (32 MiB) |

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

Overlapping workflows in one application reuse schema discovery for an identical
snapshot manifest. Each workflow receives its own schema copy, including nested
values returned to native callers. The cache holds weak references, so entries
can be collected when workflows release their references. A changed snapshot
gets a separate entry. Workflow setup pins canonical and property tables together
under the database coordinator before discovering or reusing the schema.

Quotas count retained state. Snapshot admission includes canonical Arrow tables,
property projections, schema and lookup structures. Handle charges include rows,
metadata, lineage and the most recent fetched page. Shared objects can be charged
again across handles; these are approximate accounting bytes, rather than RSS.
Each workflow is conservatively charged for its schema copy and the complete
shared schema entry. The cache is scoped to one application instance.
A tool exceeding its quota rolls back its new handles and lineage. Existing
handles and the previous fetched page remain usable. `allow_large` cannot bypass
server quotas. Delete a workflow to release its resources; idle expiry and
shutdown also release its budget.

Call rollback journals only newly created handles, replaced lineage entries,
and modified resource charges. Row and handle-byte totals are maintained
incrementally, so checkpointing and quota finalization do not copy or sum the
entire workflow's maps. Failed calls remove their new handles and restore replaced lineage, the previous
fetched page, and resource accounting. Neutral preparations in
the optional read cache retain their documented lifecycle. Tool execution,
payload construction and explicitly enumerating all handles can still grow
with the amount of data involved.

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

## Adjacency lookups

ANA traversal tools pin a compact index from the shared graph engine when its
conservative reservation fits the configured ceiling and the remaining workflow
and server byte budgets. Otherwise they use the scan path. The reservation is
included in `snapshot_bytes`; it is computed from edge count, node count and
relationship types, rather than always reserving the full 8 MiB ceiling. It
reduces the budget available for handles. Set
`GRAPHARROW_ANA_ADJACENCY_INDEX_MAX_BYTES_PER_SESSION=0` to disable it for new
workflows, or raise the ceiling to admit larger indexes. The application-wide
`GRAPHARROW_ADJACENCY_INDEX_MAX_BYTES` ceiling must also admit the entry.

The first participating traversal builds outgoing and incoming lists of canonical edge-row
positions, grouped by relationship type and endpoint. Single-source lookups
binary-search endpoint keys. Larger frontiers use Arrow lookups over typed
endpoint keys and gather the matching positions in batches. Results retain
canonical edge order, parallel relationships, direction semantics and single
emission of self-loops in bidirectional expansions. Limited expansions still
compute complete branch membership for compatible union counts.

No handles, entity payloads or relationship-property copies are retained in the
index. A completed index may remain after a later tool admission failure because
its capacity was already reserved. An interrupted build publishes neither
direction. Workflow shutdown releases its index reference and reservation. A
bounded application LRU can retain the prepared index for later workflows and
Cypher queries using the same immutable edge files. Unchanged files can reuse an
index across snapshots; changed files receive another entry. Handles, lineage
and entity ownership remain independent. No new disk format or persistent
dataset is required. Building the index and gathering Arrow
buffers use temporary memory beyond the retained-state reservation. See the
[shared-engine benchmark](benchmarks/shared-adjacency.md) for cross-interface
measurements and cache/transaction limits.

## Seed filters and entity ranking

Pattern execution applies validated start-variable filters before constructing
seed entity payloads. Eligible native scalar predicates use Arrow masks and
declared equality indexes. Other predicates read narrow property rows and use
the existing Python rules. Selection preserves canonical order and the scope
of an input handle; it does not apply the final result limit early. All filters
remain residual checks. Conversion failures cannot be skipped by pushing a
later predicate ahead of them, and hop validation retains its existing order.

`entity_resolve` preselects ranked finalists before decoding and scoring every
property row in Python. Supported ASCII rows use vectorized scores, context
ranking and bounded selection for hits and alternatives, with canonical order
as a tie breaker. Unicode, escaped/nonstandard aliases and nullable description
cases retain Python scoring. The selected union is rescored by the existing
implementation. Legacy/lossy or incomplete projections, small labels (fewer
than 64 nodes), long search strings (more than 64 lowercased characters), and
unsupported kernels use conservative pruning or the original scan path.

These changes apply through native ANA and HTTP without enabling result
caching. They preserve best-match classes, alternatives, reverse matches,
context scores, scalar errors and workflow-owned references. Temporary Arrow
masks and ranking tables can still grow with the label size. See the
[seed and discovery benchmark](benchmarks/ana-seed-discovery.md) for timing,
allocation measurements and limitations.

## Optional read cache

Set `GRAPHARROW_ANA_READ_CACHE_ENABLED=true` before opening new workflows to
enable a bounded LRU cache for `entity_resolve` and `value_search`. The cache
stores immutable candidate IDs and scores from the workflow's pinned snapshot.
Every call creates a new handle, records its own lineage, and passes ordinary
handle admission. Entity payloads are materialized with the workflow's owner
identity when returned. Each workflow owns its cache.

Cache keys include the operation, label, search input, context terms where
applicable, effective limit, and property/match mode for value searches.
Output variable aliases and `keep_entities` can reuse the same candidate
selection while receiving independently constructed results. Oversized entries
are served uncached. Candidate-computation errors are recomputed on later calls.
Expiry or deletion of the workflow clears its entries.

The configured capacity is reserved in full when the workflow opens and counts
inside `GRAPHARROW_ANA_MAX_BYTES_PER_SESSION` and
`GRAPHARROW_ANA_MAX_TOTAL_BYTES`. The default reservation is 4 MiB for an
enabled workflow, including when its cache is empty. This reduces the budget
available for other workflow state. Storage is allocated as entries arrive.
Completed candidate preparations may remain cached after a later handle-quota
failure because their capacity is already reserved; the failed call's handles
and lineage still roll back.

Native `resource_usage` adds `cache_used_bytes`, `cache_reserved_bytes`,
`cache_hits`, and `cache_misses` while this option is active. Used bytes are a
conservative estimate including keys and LRU container overhead. Reservations
and entries are released on workflow close. Enable this option for workloads
that repeat discovery inputs within one workflow; unique inputs still require
candidate computation while the full cache capacity remains reserved. Use
[the cache benchmark runner](../scripts/benchmark_ana_read_cache.py) to compare
cold and repeated queries with caching enabled and disabled in your environment.

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
