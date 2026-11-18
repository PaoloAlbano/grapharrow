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

Each commit prepares immutable Parquet files, a catalog version when needed,
and a manifest that names the complete snapshot. A single update to
`current_snapshot.json` makes the commit visible. Catalog-only changes and
compaction publish through the same pointer. Each attempt uses unique object
names, so another writer cannot overwrite the winner's files.

Readers capture the head once per operation. Graph records, catalog and
derived indexes come from the same manifest. ANA keeps that snapshot for the
workflow. Bolt explicit transactions keep their captured base and include
their own pending writes.

On native local storage, GraphArrow coordinates writers across threads and
processes with a reentrant lock and an OS lock at `.grapharrow-writer.lock`.
File URI aliases share the same lock. GraphArrow flushes new snapshot files
and the temporary pointer before replacing the head with an atomic rename.
It also flushes their directories on platforms that support directory
`fsync`. The filesystem must support advisory locks and atomic rename.

On object storage, any replica can prepare a write. S3 and Azure Blob
compare the head's ETag before replacing it. Google Cloud Storage compares
the object generation. Bootstrap uses a conditional create. All replicas
must use the same writable bucket or container and database prefix.
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
lost, GraphArrow checks the published ancestry for that commit. When the
result remains ambiguous, it raises `CommitOutcomeUnknownError`. REST
returns HTTP 500 with `COMMIT_OUTCOME_UNKNOWN`. Verify the outcome before
retrying a non-idempotent write. A client retry after the serving process or
connection has been lost starts a new operation.

## Maintenance and Existing Databases

Vacuum preserves the files referenced by published snapshots so in-flight
readers can still load them. On native local storage it can remove
unpublished manifests and Parquet files while holding the writer lock.
On object storage it only removes abandoned manifest and Parquet files from
generations that can no longer publish. Published history and its referenced
data accumulate until a snapshot retention policy is available.

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
