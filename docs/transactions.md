# Transactions, Sessions and Write Visibility

GraphArrow publishes graph state through snapshot manifests and a current
snapshot pointer. Session behavior depends on the interface.

| Interface | Read visibility | Write behavior |
| --- | --- | --- |
| Embedded `session()` / `read_transaction()` | Each query reads the current published state | Writes rejected |
| Embedded `write_transaction()` | Each operation reads the current published state | Each successful operation publishes immediately; no block-wide rollback |
| REST Cypher and imperative writes | Current published state per request | Successful write calls publish their results |
| MCP Cypher `query` / `write_cypher` | Current published state per call | `query` rejects writes; successful `write_cypher` calls publish |
| Bolt autocommit | Current published state for the statement | Successful write statements publish |
| Bolt explicit transaction | Transaction-local state, including its own writes | Changes are held in memory until `COMMIT`; `ROLLBACK` discards them |
| ANA workflow | Snapshot captured when the workflow is created. A new workflow sees later commits | Read-only tools |

## Cypher Statement Atomicity

An updating graph statement stages its changes in memory, including the
catalog entries for new labels and relationship types. Later clauses in
the same statement can read those changes. Errors during compilation,
execution, constraint validation or result evaluation leave the graph
and catalog as they were before the statement. A successful autocommit
statement publishes at most one snapshot.

Within an explicit Bolt transaction, a failed statement discards its own
changes. The connection enters Bolt's failed state and the client can
roll back or reset the transaction. See
[Writer Coordination and Storage Limits](#writer-coordination-and-storage-limits)
for publication failures and storage durability.

## Embedded Context Managers

An embedded `Session` controls access mode and lifetime. Exiting its `with`
block closes it. Later operations raise `TransactionError`. Calling `close()`
again has no effect.

Each successful write publishes immediately. If a later statement in the
same session raises an error, the earlier write remains visible. A read
session reads the current snapshot on each query.

The current explicit multi-statement transaction implementation is used by the
Bolt interface. Do not use an embedded write-session context manager when an
application requires block-wide rollback semantics.

## Bolt Explicit Transactions

`BEGIN` constructs transaction-local graph state. Queries in that transaction
see those local changes. `COMMIT` publishes the final state; `ROLLBACK`, reset
or connection cleanup discard an active transaction. Driver commit/rollback
workflows are covered by the [Bolt integration tests](../tests/integration/test_bolt_driver.py).

Transaction-local state is held in process memory. It is not a durable write-ahead
log, and an uncommitted transaction is not recovered after a process crash.

## Atomic Publication

On Native, each commit prepares immutable Parquet files, a catalog version when needed,
and a manifest that names the complete snapshot. A single update to
`current_snapshot.json` makes the commit visible. Catalog-only changes and
compaction publish through the same pointer. Each attempt uses unique object
names, so another writer cannot overwrite the winner's files.

On Delta, one table log commit publishes graph mutations, catalog updates, and
REST receipts. Independent writer processes can share that log. Each commit
requires its original base version. See [Storage Backends](storage-backends.md).

Readers capture the head once per operation. Graph records, catalog and
derived indexes come from the same manifest. ANA keeps that snapshot for the
workflow. Bolt explicit transactions keep their captured base and include
their own pending writes.

For Native deployments, route mutations and write maintenance to one
writer process per database. Other processes can read from the same
storage while the writer publishes new snapshots.

On native local storage, GraphArrow coordinates writers across threads and
processes with a reentrant lock and an OS lock at `.grapharrow-writer.lock`.
File URI aliases share the same lock. GraphArrow flushes new snapshot files
and the temporary pointer before replacing the head with an atomic rename.
It also flushes their directories on platforms that support directory
`fsync`. The filesystem must support advisory locks and atomic rename.

With Native on object storage, S3 and Azure Blob compare the head's ETag before replacing
it. Google Cloud Storage compares the object generation. Bootstrap uses a
conditional create. All replicas must use the same bucket or container and
database prefix.
S3-compatible endpoints must enforce AWS conditional PUT semantics.
[Amazon S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) ·
[Azure Blob concurrency](https://learn.microsoft.com/en-us/azure/storage/blobs/concurrency-manage) ·
[Google Cloud Storage preconditions](https://cloud.google.com/storage/docs/request-preconditions)

## Conflicts and Lost Responses

Two writers can prepare changes from the same snapshot. The first writer to
advance the head commits. The other receives `WriteConflictError` and can
start a new transaction to recompute its change. This applies even when the
writes touch different graph records.

REST returns HTTP 409 and `WRITE_CONFLICT` for a rejected write. Bolt
reports `Neo.TransientError.Transaction.Outdated` and closes the conflicting
explicit transaction. The embedded API raises `WriteConflictError`.
[Neo4j status-code reference](https://neo4j.com/docs/status-codes/current/errors/all-errors/)

If the storage provider may have accepted the update before the response was
lost, GraphArrow checks the published ancestry on Native or the exact Delta log
version for that commit. When the
result remains ambiguous, it raises `CommitOutcomeUnknownError`. REST
returns HTTP 500 with `COMMIT_OUTCOME_UNKNOWN`. For a write without an
idempotency key, verify the outcome before retrying.

## Retrying REST Cypher Writes

Send `Idempotency-Key` with a Cypher data write to
`POST /v1/query`. Choose a unique key for each intended write and reuse it
with the same statement text and JSON parameter values on every attempt.
Keys contain 1–128 printable ASCII characters without spaces and are scoped
to the database. Every REST replica serving keyed writes must support the header.

GraphArrow publishes the successful response and the graph change in the
same snapshot. A retry that finds the committed key returns the original
response, even from another replica or after subsequent commits. Using the
same key with a different query or parameters returns HTTP 409 with
`IDEMPOTENCY_KEY_CONFLICT`. A concurrent write using another key can still
cause `WRITE_CONFLICT`. Retry that request with its original key.

Successful updates that match zero rows also record a response. Queries that
fail before publication leave the key available. Keyed requests accept data
updates and JSON responses of up to 64 KiB. Read queries and schema commands
should omit the header. Native keeps receipts in published manifests and uses
an immutable index split by the first byte of the key hash. The database head
publishes the index root together with the snapshot. An indexed lookup reads at
most one index root and one shard before reading the receipt manifest. A keyed
commit replaces the root and the affected shard. When opening an older head
without an index, the first new keyed commit builds one from published history.
Delta stores receipts in partitions keyed by that same hash prefix.
Keyed writes use the transaction path so GraphArrow can prepare the response
before publication. Successful keys and their responses are retained for the
lifetime of the database.

## Maintenance and Existing Databases

Vacuum preserves the files referenced by published snapshots so in-flight
readers can still load them. On native local storage it can remove
unpublished manifests, index pages and Parquet files while holding the writer
lock. On object storage it only removes abandoned files from generations that
can no longer publish. Published snapshot metadata, index pages and referenced
data are retained. A reader can continue to use an earlier snapshot while
another replica commits or runs vacuum.

### Previewing Snapshot Retention

Estimate the space a retention policy could reclaim with a read-only preview.

```sh
grapharrow retention-plan /data/grapharrow --keep-last 100 --min-age-hours 168
```

The embedded equivalent is `database.plan_retention(keep_last=100,
min_age_hours=168)`. The authenticated REST endpoint is
`GET /v1/admin/retention/plan?keep_last=100&min_age_hours=168`.

The preview captures one published head. It retains at least the requested
number of snapshots and every snapshot within the requested age window. It
also retains the snapshots between them so the retained range is contiguous.
`min_age_hours=0` uses the count alone. Use the report's head and evaluation
time to identify the view that was measured.

For a known pinned snapshot, pass `--protect-from-snapshot-id ID` on the CLI,
or `protect_from_snapshot_id=ID` in Python or as a REST query parameter. The
preview includes that snapshot and every newer snapshot. Supply the lowest
snapshot ID if several readers are in use. The caller supplies this boundary
and remains responsible for tracking readers on other replicas.

`shared_parquet_paths` lists files referenced by both parts of the history.
`candidate_files` contains older-only Parquet files with their actual stored
sizes. Missing candidates are reported separately and contribute zero to
`estimated_reclaimable_bytes`. On Native, the estimate covers graph Parquet
files. On Delta, it covers Parquet files referenced by the commit log,
including older graph metadata and receipt files outside the retained range.
Delta uses commit timestamps from the log and checks the complete history
through the captured version. Logs, checkpoints and untracked files are
outside the estimate.

The preview performs no writes. Its candidates are a space estimate for
the proposed policy. Native vacuum preserves the files of all published
snapshots, including those outside that policy. On a newly created Delta
database with managed retention, the
[coordinated cleanup command](storage-backends.md#applying-managed-delta-retention)
publishes the minimum readable snapshot before removing older Parquet.

Upgrade all writers sharing a database together. When opening a database
from the earlier layout, GraphArrow reads the catalog version referenced by
the published snapshot. Inspect any newer entries in the legacy catalog
pointer before upgrading, since schema-only work from the earlier layout
may require replay. New databases use a single head from initialization.

See [the cross-interface comparison](interface-comparison.md) for visibility
examples and [live storage testing](real-object-storage-testing.md) for
provider integration checks.


## Selective Autocommit Property SET

Eligible relationship SET statements by ID can read and replace only the touched
canonical range and property projection. They use the same writer coordinator,
immutable objects and conditional snapshot publication as other commits. The
catalog, nodes and topology must remain unchanged; broader queries and explicit
transactions use their existing transaction path. Result construction precedes
publication, including when the result is empty because of SKIP or LIMIT.
See [selective relationship updates](engine-performance.md#selective-relationship-property-updates).
