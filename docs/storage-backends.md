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
metadata, and REST idempotency receipts. General mutations replace the metadata
partition and any changed canonical partitions in a single log commit. A
catalog-only change leaves the canonical partitions in place. Receipt keys are
bucketed by hash prefix. A keyed REST write publishes its response in the same
commit as its graph updates.

New Delta databases partition edges into ranges of 8,192 IDs by default.
A compatible autocommit `SET` by edge ID reads and replaces only the affected
edge range. This path applies to updates that preserve existing scalar
property types on graphs without constraints when the request carries no REST
idempotency key. The full graph write path also reuses unchanged edge range
files. Property projections are derived from the graph version selected by
each reader.

`GRAPHARROW_EDGE_PARQUET_RANGE_SIZE` overrides the range width at database
creation or explicit compaction. Set it to `0` for a single edge partition.
The chosen width is stored with the graph. Compaction publishes a new snapshot
with the configured width and preserves historical reads.
The GraphArrow Delta metadata format is `grapharrow-delta-v1`.

Readers capture one Delta version. Catalog, adjacency and property projections
are resolved from that version, including after another replica commits. New
readers can capture the next version. Keep ANA sessions on the replica that
issued their session ID because handles live in the server process.

Canonical tables and derived property projections share the Arrow table cache
budget. Tables that share Arrow buffers are charged once for those buffers.
A new snapshot capture checks the latest log version before reusing a
pinned read handle or a table. Cypher reuses typed projections of unchanged
edge range files and ANA reuses their canonical Arrow tables across snapshots.
ANA also retains one recent schema and reuses relationship statistics for
unchanged edge ranges within its schema cache budget. Unchanged node files
can share node tables, label projections, observed node properties, and node
lookups across Delta snapshots. `GRAPHARROW_ANA_NODE_LOOKUP_CACHE_MAX_BYTES`
bounds the most recent Delta node lookup retained between sessions. It
defaults to 16 MiB. Set it to `0` to retain lookups only while sessions use them.

A general mutation publishes only against its captured base version. For
compatible point `SET` operations on disjoint edge ranges, Delta can rebase
the commit without replaying the query as long as the graph metadata remains
unchanged. A conflicting file rewrite or a change to graph metadata, including
catalog updates and compaction, rejects the pending point update. The Delta
log identifies point commits by a unique ID so the writer can check whether
publication succeeded if the storage response is lost.

When capturing a snapshot, a replica can reuse its cached Delta manifest if
the next commit file is absent. Each capture checks storage directly. A new
commit triggers a fresh table open, and workflows already pinned keep reading
their original versions.

REST reports write conflicts as HTTP 409. Retry the whole operation in a new
transaction. A retry with the same REST idempotency key and payload replays a
committed response byte for byte. See [Transactions and Sessions](transactions.md).

### Inspecting Delta storage

```sh
GRAPHARROW_STORAGE_BACKEND=delta grapharrow storage-inventory /data/delta-graph
```

The same report is available from `database.storage_inventory()` in Python and
`GET /v1/admin/storage/inventory` on the server. The command requires an
existing Delta database. For object storage, configure the credentials and
endpoint through the GraphArrow storage settings so both the filesystem
listing and Delta log reads can access the same location.

The report includes file counts, logical sizes in bytes, and the oldest and
newest modification times when storage supplies them. It distinguishes observed
Parquet files referenced by the captured version, files with a recorded remove
action, and other observed Parquet files in Delta data partitions. It also counts
missing active files, commit JSON through the captured version, and classic
checkpoints. `commits_since_checkpoint` counts the commit JSON after the newest
observed checkpoint at or before the captured version, or all commits if there
is no checkpoint. Delta versions start at zero and GraphArrow snapshot IDs
start at one.

The inventory lists storage and can read the complete commit history. Use it
for occasional inspection of a busy database. An untracked file may belong to
a writer that is staging data or published after the captured version. Treat
all file categories as accounting data. Eligibility for deletion depends on
the retention policy and readers or writers using those files.

The [retention preview](transactions.md#previewing-snapshot-retention)
classifies older Delta Parquet files against a proposed retention window.
A caller that knows the oldest snapshot still in use can include it with
`--protect-from-snapshot-id`. The preview uses one captured history and
preserves it in storage.

Set `GRAPHARROW_DELTA_CHECKPOINT_INTERVAL` to choose the number of commits
between automatic checkpoints when initializing a new Delta database.
GraphArrow stores the interval in the Delta table so replicas use the same
value. Without an explicit value it uses the Delta library default. Shorter
intervals reduce the amount of log replay on table open and create more
checkpoint files.

### Incremental snapshot refresh

Set `GRAPHARROW_DELTA_INCREMENTAL_REFRESH_ENABLED=true` to try incremental
replay on a Delta replica with the snapshot table cache enabled. The replica
keeps a private Delta cursor and captures an independent view of each selected
version. It builds Arrow fragments for the requested partitions when it reads
them. An unsupported layout or a replay error falls back to opening the
complete Delta table.

The setting applies per process, leaves the on-disk layout intact, and is
disabled by default. Freshness checks and retention pins still apply.

### Applying managed Delta retention

Enable managed retention when creating a new Delta database. All replicas
opening that database participate automatically. Use a GraphArrow version
that supports managed retention on every replica and the `s3` or `azure`
extra when the database uses that provider. The provider SDK supplies
conditional reads and writes for the snapshot admission record.

```sh
export GRAPHARROW_STORAGE_BACKEND=delta
GRAPHARROW_DELTA_MANAGED_RETENTION=true grapharrow init /data/managed-graph
```

Run `retention-plan` to choose the first retained snapshot and
`storage-inventory` to check the current floor, the oldest live pin and any
pending cleanup. If the first retained snapshot is 250, run:

```sh
grapharrow retention-apply /data/managed-graph --minimum-snapshot-id 250
```

Managed retention uses a distinct GraphArrow layout so older GraphArrow
versions reject it. For an existing unmanaged database, create a new
managed database when migrating.

For a maintenance window, stop new work and close existing sessions first.
The command requires all pins to be released and atomically closes
admission before deleting files. With `--online`, it allows already pinned
and incoming work at or above the proposed floor. A pin below that floor
blocks the operation. Pins persist until released, including while a
replica is suspended. Recover abandoned pins from a fenced replica before
advancing past them. An ANA workflow or a point `SET` holds its pin from
snapshot selection through completion. A replica shares the pin across
concurrent operations that use the same snapshot.

The floor is durable before any Parquet file is removed. Requests for
snapshots below it receive an expiration error. If cleanup is interrupted,
rerun the command with the same floor and mode to finish. Log JSON,
checkpoints, untracked files and all Parquet required by retained snapshots
stay available for Delta replay and live requests. The Python equivalent
is `database.apply_retention(minimum_snapshot_id=250, online=False)`.

#### Recovering pins from a stopped replica

`storage-inventory` reports the replica ID, unique incarnation ID, snapshot ID,
acquisition time and age of every active pin. Set
`GRAPHARROW_DELTA_REPLICA_ID` to a workload identity such as the Pod UID.
GraphArrow uses the hostname when no identity is configured and generates
a new incarnation ID for each coordinator lifetime.

For a database on a POSIX local disk shared by processes on one host,
GraphArrow checks the kernel lock associated with the incarnation. A
suspended process holds its lock. Once the old process has stopped, run:

```sh
grapharrow retention-recover-pins /data/managed-graph --incarnation-id "$INCARNATION"
```

Keep the local incarnation lock files as part of the database. For object
storage, supply `--verifier /absolute/path/to/verifier`. The verifier is a
trusted executable supplied by the deployment operator. It must confirm
that the exact incarnation can issue no further storage operations and
that operations already sent to the provider have finished or been
cancelled. Treat a Pod disappearing from the orchestration API as
insufficient evidence. A timeout or a negative result leaves the pins
protected.

The executable receives a JSON object on standard input containing
`database_id`, `database_location`, `replica_id`, `incarnation_id` and a
fresh `challenge`. For a verified fence, return those fields unchanged
with JSON booleans `"fenced": true` and `"drained": true` on standard output
and exit successfully. GraphArrow records the fence in the same conditional
update that removes the pins. Later admissions by that incarnation are
rejected. The command releases only the identified incarnation's pins.

## Current Delta scope

- Node and edge changes publish the affected ID ranges. General mutations
  still check the shared graph version, so they can conflict even when
  they change disjoint data. See [Selective node range publication](engine-performance.md#selective-node-range-publication).
- `verify` checks the captured metadata and reads active Parquet data.
  `compact` rewrites the active graph partitions as a new snapshot.
  Managed Delta retention controls when older snapshots expire. `vacuum`
  is currently available for Native storage. Use GraphArrow as the sole
  writer to its table, including for maintenance.
- Integration tests cover local disk and Moto S3. Simulated object-store
  tests kill a writer after staging Parquet files, and again after the
  provider accepts the log commit but before the writer receives its
  response. In both cases, readers and REST retries see an atomic graph
  and receipt. The same tests were verified with Azurite 3.37. Tests
  against real S3 and Azure accounts are pending.

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
