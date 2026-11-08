# Transactions, Sessions and Write Visibility

GraphArrow publishes graph state through snapshot manifests and a current
snapshot pointer. Session behavior depends on the interface; the names of
embedded helper methods do not imply identical semantics to Bolt transactions.

| Interface | Read visibility | Write behavior |
| --- | --- | --- |
| Embedded `session()` / `read_transaction()` | Each query reads the current published state | Writes rejected |
| Embedded `write_transaction()` | Each operation reads the current published state | Each successful operation publishes immediately; no block-wide rollback |
| REST Cypher and imperative writes | Current published state per request | Successful write calls publish their results |
| MCP Cypher `query` / `write_cypher` | Current published state per call | `query` rejects writes; successful `write_cypher` calls publish |
| Bolt autocommit | Current published state for the statement | Successful write statements publish |
| Bolt explicit transaction | Transaction-local state, including its own writes | Changes are held in memory until `COMMIT`; `ROLLBACK` discards them |
| ANA workflow | Snapshot captured when the workflow is created | All current tools are read-only; create a new workflow to see later commits |

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

## Writer Coordination and Storage Limits

On native local storage, GraphArrow coordinates mutations through a reentrant
thread lock and an OS file lock at `.grapharrow-writer.lock` in the database
root. Canonical paths share a coordinator, including local file-URI aliases.
Separate REST, Bolt, CLI, and embedded processes using the same database path
serialize their graph and catalog writes. Index declarations, compaction,
vacuum, and recovery use the same coordinator. The lock file remains in place
after release, and the OS releases a process's lock when its descriptor closes
or the process exits.

Autocommit mutations hold the coordinator across reading their base state,
validation, and publication. Explicit Bolt transactions keep transaction-local
state and acquire the publication lock at commit. Before writing any candidate
files, commit verifies that both the base snapshot and catalog still match.
Another commit, a catalog-only change, or compaction can invalidate that base.
The caller then receives `WriteConflictError`, a subclass of
`TransactionError`, and must recompute the write in a new transaction.

REST maps write conflicts to HTTP 409 with error code `WRITE_CONFLICT`.
Bolt reports `Neo.TransientError.Transaction.Outdated`, which retry-aware
drivers classify as a transient transaction failure. The conflicting explicit
transaction is closed. [Neo4j status-code reference](https://neo4j.com/docs/status-codes/current/errors/all-errors/)

Direct catalog publication requires a newer version, with an identical current
catalog treated as an idempotent retry. Snapshot publication advances the
snapshot ID and checks a declared parent against the current head. Repeating
the identical current snapshot returns its existing pointer. Published
manifests are protected against replacement with different contents.

For object storage, coordination covers threads in one process using the same
database URI. Independent processes need one externally coordinated writer.
Provider conditional writes and distributed locking are outside the current
object-store publication contract.

On the local filesystem, GraphArrow writes `current_snapshot.json` to a
temporary file in the same directory, flushes it to disk and replaces the
current file with `os.replace`. A reader sees either the previous complete
pointer or the new one. An error before replacement leaves the previous
pointer in place.

This guarantee covers pointer visibility. Power-loss durability of the
whole snapshot, including the manifest and Parquet files, remains outside
that guarantee. Object storage continues to publish through its filesystem
output stream, with visibility depending on the backend.

The catalog and snapshot have separate heads. Schema declarations can remain
visible after a later graph-persistence failure. Verification, recovery, and
vacuum support inspection and cleanup of unpublished data. Schedule vacuum
between ordinary Cypher read operations because those reads can load derived
files after capturing a manifest. ANA workflows pin canonical and property
tables together during setup and keep their in-memory snapshot after vacuum.

Use [verification and recovery operations](../README.md) for the supported
administrative checks, and consult the
[cross-interface comparison](interface-comparison.md) for executable visibility
examples. The writer-coordination tests additionally exercise simultaneous
processes, bootstrap races, concurrent MERGE, stale explicit transactions,
catalog changes, compaction, process termination, and inherited lock state.
The native process-lock paths are exercised on macOS and Linux. Windows has
a byte-range lock implementation that was not exercised in this validation.
