# Storage Backend

A Storage Backend owns graph snapshot access, atomic publication and maintenance.
The graph engine uses typed contracts for catalog, snapshot and graph stores.
The current implementation is `NativeStorageBackend`.

## Configuration

`GRAPHARROW_STORAGE_BACKEND` defaults to `native`. It is currently the only
supported value. Other values fail settings validation before the application
opens the database.

```sh
GRAPHARROW_STORAGE_BACKEND=native
GRAPHARROW_DATABASE_PATH=/data/grapharrow
```

The database path identifies the physical storage location. For example,
`s3://my-bucket/my-db` selects S3 access through the filesystem adapter while
retaining the native backend. Credentials and endpoints remain provider settings.
See [Dependencies and Installation](dependency-compatibility.md) and
[Real Object Storage Testing](real-object-storage-testing.md).

## Application construction

`build_application()` invokes `create_storage_backend()` once. The factory retains
the supplied filesystem, Parquet and Arrow adapters. Its stores are shared by the
engine, index operations and interface-facing repository view. The same writer
coordinator governs catalog changes, graph commits and maintenance.

`StorageBackend` is a Python `Protocol`. Its contracts include:

- snapshot capture and catalog access;
- canonical and property-table reads, filtered projections and batch iteration;
- publication of complete graph changes;
- an optional selective relationship-property edit, which can decline to the
  complete transaction path;
- initialization, verification, recovery, compaction, vacuum and retention previews.

The graph engine handles query execution and validation. The native backend owns
its immutable object publication and selective range replacement. Native
maintenance owns filesystem traversal and cleanup. Store implementations resolve
their dataset references and select the applicable label and adjacency data.

The application exposes the selected instance as `application.storage_backend`.
`application.repositories` continues to refer to the same store instances.

## Native behavior

The extraction preserves the native manifest, catalog and Parquet layout. Existing
databases reopen without conversion. Snapshot references, checksums, Arrow buffer
reuse and caches retain their existing behavior; there is no additional backend
metadata file in this refactor.

Atomic commits still publish through the database head, and explicit transactions
still stage until commit. Selective SET keeps its result-before-publication and
conflict checks. See [Transactions and Sessions](transactions.md) for the interface
contracts, and [Engine Performance](engine-performance.md) for selective updates.

## Verification

[Storage Backend contract tests](../tests/unit/test_storage_backends.py) check
configuration, adapter and writer sharing, publication delegation, maintenance,
format preservation and reopening. Existing persistence, concurrency, rollback,
cache and interface tests exercise the native implementation through the factory.
