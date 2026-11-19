# Storage Backends

The Storage Backend controls how GraphArrow stores and publishes graph snapshots.
Cypher, ANA, REST, Bolt, MCP, and the embedded API use the same graph engine and
select the backend when the application starts.

| Backend | Graph storage | Writer deployment |
| --- | --- | --- |
| `native` (default) | Immutable Parquet files and a conditionally updated snapshot head | One writer instance and any number of readers |
| `delta` (experimental) | One Delta Lake table per graph | Independent replicas can write through the same Delta transaction log |

The database path selects the physical storage. Use a filesystem path, an
`s3://` URI, or an `az://` URI with Delta. Native also supports `gs://` URIs.
Use the same backend and database URI on every replica. Start a Delta database at
a new path. The Native and Delta formats require separate database roots.

## Delta Lake setup

The default runtime Docker image includes Delta support. For a source checkout,
install the optional extra and select the backend:

```sh
GRAPHARROW_STORAGE_BACKEND=delta uv run --locked --extra delta grapharrow-server --path /data/delta-graph
```

For a release wheel, install `pip install './grapharrow-<version>-py3-none-any.whl[delta]'`
and set `GRAPHARROW_STORAGE_BACKEND=delta` in the environment of the server, Bolt
process, CLI, or embedded client. Embedded clients can also pass
`GraphArrowSettings(storage_backend="delta")` to `Database`.

Delta uses the delta-rs object store client. Set the S3 and Azure credentials
with the [GraphArrow storage settings](../README.md#real-object-storage) or the
credential environment variables supported by delta-rs. S3 needs atomic
create-if-absent writes for `_delta_log` objects. GraphArrow selects the ETag
conditional mode for S3-compatible storage.

## Atomicity and concurrency

The table at `<database path>/delta` partitions canonical nodes, edges, graph
metadata, and REST idempotency receipts. Every mutation replaces the metadata
partition and any changed canonical partitions in a single log commit. A
catalog-only change leaves the canonical partitions in place. Receipt keys are
bucketed by hash prefix. A keyed REST write publishes its response in the same
commit as its graph updates.

Readers capture one Delta version. Catalog, adjacency and property projections
are resolved from that version, including after another replica commits. New
readers can capture the next version. Keep ANA sessions on the replica that
issued their session ID because handles live in the server process.

Canonical tables and derived property projections share the Arrow table cache
budget. A new snapshot capture checks the latest log version before reusing a
pinned read handle or a table. ANA retains one recent schema and reuses
relationship statistics for unchanged edge ranges within its schema cache budget.

A writer publishes only against its captured base version. When two replicas
write from the same base, one publishes and the other receives a write conflict.
REST reports HTTP 409. Retry the whole operation in a new transaction. A retry
with the same REST idempotency key and payload replays a committed response
byte for byte. See [Transactions and Sessions](transactions.md).

## Current Delta scope

- Node or edge changes can rewrite the corresponding whole partition. Property
  projections are rebuilt from canonical rows. This first version guarantees
  atomicity and snapshot isolation with conservative conflicts, including for
  writes to different parts of the graph.
- `verify` checks the captured metadata and reads active Parquet data.
  `compact` rewrites the active graph partitions as a new snapshot.
  Historical reads remain available. `vacuum` and retention planning are
  currently available only with Native. Use GraphArrow as the sole writer to
  its table, including for maintenance.
- Integration tests cover local disk and Moto S3. A separate local test also
  verified Azure Blob behavior with Azurite 3.37. Tests against real S3 and
  Azure accounts are pending.

## Native behavior and storage contracts

Native keeps its existing Parquet layout and snapshot history. It serializes
writes through one writer instance and uses conditional head publication.
Selective relationship property updates use immutable Parquet ranges. See
[Engine Performance](engine-performance.md) for the selective path and
[Dependencies and Installation](dependency-compatibility.md) for cloud extras.

The application exposes the selected backend as `application.storage_backend`.
Catalog, snapshot and graph contracts remain shared by the engine and public
interfaces. Backend implementations resolve dataset references and handle
publication and maintenance. [Concurrent Delta tests](../tests/integration/test_delta_backend.py)
and the [interface comparison](../tests/integration/test_interface_comparison.py)
verify these behaviors.
