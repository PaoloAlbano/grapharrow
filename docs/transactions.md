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

Use a single coordinated writer for each database path. The current publication
path does not provide optimistic conflict detection or a lock shared by
independent processes. Concurrent writers can overwrite each other's published
state; separate REST and Bolt processes do not automatically coordinate writers.
An explicit Bolt transaction also does not detect a later commit from another
writer before publishing its own state.

Publishing a snapshot pointer does not make an arbitrary set of filesystem or
object-storage writes one distributed atomic transaction. Object-store behavior
depends on the backend. The current pointer is written through the generic
filesystem output stream; atomic replacement and crash-durable publication are
not guaranteed, including for readers racing a local pointer write.
General ACID guarantees, multi-writer serialization,
durable recovery of in-flight transactions and full database isolation levels
have not been established.

Use [verification and recovery operations](../README.md) for the supported
administrative checks, and consult the
[cross-interface comparison](interface-comparison.md) for executable visibility
examples. It checks sequential writes and ANA snapshot stability; it is not a
concurrent-writer correctness test.
