"""An optional Delta Lake backend with one atomic transaction per graph commit."""

from __future__ import annotations

from collections import Counter
from collections.abc import Generator, Iterator
from concurrent.futures import Future
from contextlib import AbstractContextManager, ExitStack, contextmanager, nullcontext
from copy import deepcopy
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from functools import cached_property
from pathlib import Path
from threading import Lock, RLock, local
from time import monotonic_ns, sleep
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import uuid4

import orjson
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]
import pyarrow.dataset as ds  # type: ignore[import-untyped]
import pyarrow.fs as pafs  # type: ignore[import-untyped]
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, DeltaError, TableNotFoundError
from deltalake.fs import DeltaStorageHandler
from deltalake.transaction import CommitProperties

from grapharrow.adapters import FileSystemAdapter
from grapharrow.adapters.snapshot_tables import SnapshotTableCache
from grapharrow.adapters.writer import WriterLock, writer_locked
from grapharrow.config import GraphArrowSettings
from grapharrow.core import DatabaseLayout, property_codec
from grapharrow.core.errors import (
    CatalogError,
    CommitOutcomeUnknownError,
    StorageError,
    WriteConflictError,
)
from grapharrow.domain import (
    CatalogVersion,
    CurrentSnapshotPointer,
    DatasetFile,
    SnapshotId,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.domain.records import EdgeRecord, NodeRecord
from grapharrow.domain.snapshot import IdempotencyReceipt
from grapharrow.repositories.catalog import deserialize_catalog, serialize_catalog
from grapharrow.repositories.graph import (
    EDGE_SCHEMA,
    NODE_RANGE_SIZE,
    NODE_SCHEMA,
    _can_patch_properties,
    _edge_ranges,
    _edges_table,
    _id_ranges,
    _nodes_table,
    node_records_from_table,
)
from grapharrow.storage.contracts import CatalogStore, GraphStore, SnapshotStore, StorageMaintenance
from grapharrow.storage.delta_graph import DeltaEdgeRange, DeltaGraphStore, dataset_ref
from grapharrow.storage.delta_inventory import inspect_delta_storage, plan_delta_retention
from grapharrow.storage.delta_read_view import DeltaReadView
from grapharrow.storage.delta_retention import MANAGED_DESCRIPTION, DeltaRetentionCoordinator
from grapharrow.storage.models import (
    DeltaPinRecoveryResult,
    DeltaRetentionResult,
    DeltaStorageInventory,
    InitializationResult,
    PointWriteReceipt,
    RecoveryReport,
    RetentionPlan,
    VerificationReport,
)

if TYPE_CHECKING:
    from grapharrow.use_cases.graph import GraphState

_FORMAT = "grapharrow-delta-v1"
_MANAGED_FORMAT = "grapharrow-delta-managed-v1"
_POINT_COMMIT_ID = "grapharrowPointCommitId"
_POINT_COMMIT_RETRIES = 10
_POINT_RECOVERY_LOG_PROBES = 8
_RECEIPT_REUSE_MAX_ROWS = 1024
_RECEIPT_REUSE_MAX_PARQUET_BYTES = 1024 * 1024
_DEFAULT_EDGE_RANGE_SIZE = 8192
_HEAD_PROBE_BATCH_SECONDS = 0.001
# The string interval contains exactly the partition names with the 'edges-' prefix.
_EDGE_PARTITION_PREDICATE = "kind = 'edges' OR (kind >= 'edges-' AND kind < 'edges.')"
_NODE_PARTITION_PREDICATE = "kind = 'nodes' OR (kind >= 'nodes-' AND kind < 'nodes.')"
_SCHEMA = pa.schema(
    [
        ("kind", pa.string()),
        *NODE_SCHEMA,
        *[field for field in EDGE_SCHEMA if field.name not in NODE_SCHEMA.names],
        ("key_hash", pa.string()),
        ("request_hash", pa.string()),
        ("response_json", pa.string()),
    ]
)
_RECEIPT_COLUMNS = ["key_hash", "request_hash", "response_json"]
_CONFLICT = "Database changed since this write began. Retry in a new transaction."
_ReadSource = DeltaTable | DeltaReadView


@dataclass(slots=True)
class _ReceiptBucket:
    snapshot_id: SnapshotId
    key_hash: str
    rows: pa.Table


@dataclass(slots=True)
class _WriterScope:
    receipt_bucket: _ReceiptBucket | None = None


class _DeltaWriter:
    """Serialize operations within one application. Delta arbitrates across replicas."""

    local_root: Path | None = None

    def __init__(self) -> None:
        self._lock = RLock()
        self._local = local()

    @property
    def scope(self) -> _WriterScope | None:
        scope: _WriterScope | None = getattr(self._local, "scope", None)
        return scope

    @contextmanager
    def hold(self) -> Iterator[None]:
        with self._lock:
            if self.scope is not None:
                yield
                return
            self._local.scope = _WriterScope()
            try:
                yield
            finally:
                del self._local.scope


@dataclass(frozen=True, slots=True)
class _Head:
    commit_id: str
    created_at: str
    node_count: int
    edge_count: int
    labels: dict[str, int]
    relationships: dict[str, int]
    receipt: IdempotencyReceipt | None
    edge_range_size: int
    metadata_version: int
    node_ranges: bool = False

    def manifest(self, snapshot_id: SnapshotId, version: CatalogVersion) -> SnapshotManifest:
        return SnapshotManifest(
            snapshot_id=snapshot_id,
            parent_snapshot_id=(
                SnapshotId(snapshot_id.value - 1) if snapshot_id.value > 1 else None
            ),
            commit_txid=snapshot_id.value,
            created_at=self.created_at,
            catalog_version=version,
            catalog_path=f"delta/{snapshot_id.value}/catalog",
            nodes=(dataset_ref(snapshot_id, "nodes", self.node_count),) if self.node_count else (),
            edges=(dataset_ref(snapshot_id, "edges", self.edge_count),) if self.edge_count else (),
            node_labels=tuple(
                dataset_ref(snapshot_id, "node_labels", count, name)
                for name, count in sorted(self.labels.items())
            ),
            node_properties=tuple(
                dataset_ref(snapshot_id, "node_properties", count, name)
                for name, count in sorted(self.labels.items())
            ),
            adjacency_by_source=tuple(
                dataset_ref(snapshot_id, "adjacency_by_source", count, name)
                for name, count in sorted(self.relationships.items())
            ),
            adjacency_by_dest=tuple(
                dataset_ref(snapshot_id, "adjacency_by_dest", count, name)
                for name, count in sorted(self.relationships.items())
            ),
            edge_properties=tuple(
                dataset_ref(snapshot_id, "edge_properties", count, name)
                for name, count in sorted(self.relationships.items())
            ),
            idempotency_receipt=self.receipt,
        )


@dataclass(frozen=True)
class _ReadVersion:
    table: _ReadSource
    manifest: SnapshotManifest
    catalog: VersionedCatalog
    head: _Head
    metadata_file: tuple[str, str | None, str] | None

    @cached_property
    def actions(self) -> pa.Table:
        try:
            actions = (
                self.table.actions
                if isinstance(self.table, DeltaReadView)
                else pa.table(self.table.get_add_actions(flatten=True))
            )
            columns = ["path", "partition.kind", "num_records"]
            if "size_bytes" in actions.column_names:
                columns.append("size_bytes")
            return actions.select(columns)
        except (DeltaError, pa.ArrowException, OSError) as exc:
            raise StorageError("Could not read the active Delta graph files.") from exc

    @cached_property
    def receipt_buckets(self) -> dict[str, tuple[int, int]] | None:
        columns = ["partition.kind", "num_records", "size_bytes"]
        if not set(columns).issubset(self.actions.column_names):
            return None
        sizes: dict[str, tuple[int, int]] = {}
        actions = self.actions.select(columns)
        receipts = actions.filter(pc.starts_with(actions["partition.kind"], "receipt-"))
        for row in receipts.to_pylist():
            kind, count, size = row["partition.kind"], row["num_records"], row["size_bytes"]
            if count is None or size is None:
                return None
            seen_count, seen_size = sizes.get(kind, (0, 0))
            sizes[kind] = seen_count + count, seen_size + size
        return sizes

    @cached_property
    def graph_files(self) -> dict[str, tuple[DatasetFile, ...]]:
        rows = self.actions.select(["path", "partition.kind", "num_records"]).to_pylist()
        files: dict[str, list[DatasetFile]] = {}
        for row in rows:
            kind = row["partition.kind"]
            if kind == "nodes" or kind.startswith(("nodes-", "edges-")):
                files.setdefault(kind, []).append(
                    DatasetFile(
                        row["path"],
                        row["num_records"] or 0,
                        "nodes" if kind.startswith("nodes") else "edges",
                    )
                )
        return {
            kind: tuple(sorted(items, key=lambda item: item.path))
            for kind, items in sorted(files.items())
        }

    @cached_property
    def edge_ranges(self) -> tuple[DeltaEdgeRange, ...]:
        if not self.head.edge_range_size or not self.head.edge_count:
            return ()
        return tuple(
            DeltaEdgeRange(kind, files)
            for kind, files in self.graph_files.items()
            if kind.startswith("edges-")
        )


@dataclass(slots=True)
class _ReadPin:
    table: _ReadSource | None
    description: _ReadVersion | None
    readers: int = 1
    load_lock: Any = field(default_factory=Lock)


class DeltaStorageBackend:
    """Use a partitioned Delta table as the sole source of truth for one database."""

    def __init__(
        self,
        *,
        layout: DatabaseLayout,
        settings: GraphArrowSettings,
        filesystem: FileSystemAdapter,
        snapshot_tables: SnapshotTableCache | None = None,
    ) -> None:
        self._layout = layout
        self._filesystem = filesystem
        self._uri = str(layout.delta_dir)
        self._options = _storage_options(layout.root, settings)
        self._writer = _DeltaWriter()
        self._point_write_table: DeltaTable | None = None
        self._edge_range_size = settings.edge_parquet_range_size
        self._checkpoint_interval = settings.delta_checkpoint_interval
        self._manage_new_table = settings.delta_managed_retention
        self._replica_id = settings.delta_replica_id
        self._managed: bool | None = None
        snapshot_tables = (
            snapshot_tables
            if snapshot_tables is not None
            else SnapshotTableCache(settings.snapshot_table_cache_max_bytes)
        )
        self._retain_read_version = snapshot_tables.max_bytes > 0
        self._incremental_refresh = settings.delta_incremental_refresh_enabled
        self._read_refresh_cursor: DeltaTable | None = None
        self._read_version: _ReadVersion | None = None
        self._read_refresh_observation: tuple[SnapshotManifest, int] | None = None
        self._read_version_lock = Lock()
        self._read_pins: dict[SnapshotId, _ReadPin] = {}
        self._read_pins_lock = Lock()
        self._read_refresh_lock = Lock()
        self._head_probe_lock = Lock()
        self._queued_head_probes: dict[SnapshotId, Future[bool]] = {}
        self._probed_commit_size: tuple[int, int] | None = None
        self._graph = DeltaGraphStore(
            self._read_partition,
            edge_ranges=self._read_edge_ranges,
            node_files=self._read_node_files,
            snapshot_tables=snapshot_tables,
            filter_pushdown_enabled=settings.parquet_filter_pushdown_enabled,
        )

    name = "delta"

    @property
    def catalog(self) -> CatalogStore:
        return self

    @property
    def snapshot(self) -> SnapshotStore:
        return self

    @property
    def graph(self) -> GraphStore:
        return self._graph

    @property
    def writer(self) -> WriterLock:
        return self._writer

    @property
    def maintenance(self) -> StorageMaintenance:
        return self

    @property
    def layout(self) -> DatabaseLayout:
        return self._layout

    def _open(self, snapshot_id: SnapshotId | None = None) -> DeltaTable:
        version = None if snapshot_id is None else snapshot_id.value - 1
        if version is not None and version < 0:
            raise StorageError("Delta snapshot IDs start at 1.")
        try:
            table = DeltaTable(self._uri, version=version, storage_options=self._options)
            if self._managed is None:
                self._managed = table.metadata().description == MANAGED_DESCRIPTION
            return table
        except TableNotFoundError:
            raise
        except (DeltaError, OSError) as exc:
            raise StorageError("Could not open the requested Delta table version.") from exc

    def _try_open(self) -> DeltaTable | None:
        try:
            return self._open()
        except TableNotFoundError:
            return None

    def _require_table(self, snapshot_id: SnapshotId | None) -> DeltaTable:
        try:
            return self._open(snapshot_id)
        except TableNotFoundError as exc:
            raise StorageError(
                "Delta graph does not exist. Initialize the database first."
            ) from exc

    def _read_partition(
        self, snapshot_id: SnapshotId, kind: str, columns: list[str], expression: object
    ) -> pa.Table:
        with self.pin_snapshot(snapshot_id):
            return _partition(self._read_table(snapshot_id), kind, columns, expression)

    def pin_snapshot(self, snapshot_id: SnapshotId) -> AbstractContextManager[None]:
        return self._pin_read_snapshot(snapshot_id)

    @contextmanager
    def _pin_read_snapshot(
        self, snapshot_id: SnapshotId, source: _ReadSource | None = None
    ) -> Iterator[None]:
        """Keep one reader-owned view until the last overlapping pin releases it."""
        if self._managed is None:
            source = self._require_table(snapshot_id)
        retention = self._retention.pin(snapshot_id.value) if self._managed else nullcontext()
        with retention:
            with self._read_pins_lock:
                pinned = self._read_pins.get(snapshot_id)
                if pinned is None:
                    recent = self._recent_read_version(snapshot_id)
                    pinned = _ReadPin(recent.table if recent is not None else source, recent)
                    self._read_pins[snapshot_id] = pinned
                else:
                    pinned.readers += 1
            try:
                yield
            finally:
                with self._read_pins_lock:
                    pinned.readers -= 1
                    if not pinned.readers:
                        del self._read_pins[snapshot_id]

    @cached_property
    def _retention(self) -> DeltaRetentionCoordinator:
        return DeltaRetentionCoordinator(self._layout, self._filesystem, self._replica_id)

    def _read_edge_ranges(self, snapshot_id: SnapshotId) -> tuple[DeltaEdgeRange, ...]:
        return self._describe_read_version(self._read_table(snapshot_id)).edge_ranges

    def _read_node_files(self, snapshot_id: SnapshotId) -> tuple[DatasetFile, ...]:
        version = self._describe_read_version(self._read_table(snapshot_id))
        return tuple(
            file
            for kind, files in version.graph_files.items()
            if kind == "nodes" or kind.startswith("nodes-")
            for file in files
        )

    def _read_table(self, snapshot_id: SnapshotId) -> _ReadSource:
        with self._read_pins_lock:
            pinned = self._read_pins.get(snapshot_id)
        if pinned is None:
            recent = self._recent_read_version(snapshot_id)
            return recent.table if recent is not None else self._require_table(snapshot_id)
        if pinned.table is not None:
            return pinned.table
        with pinned.load_lock:
            if pinned.table is None:
                recent = self._recent_read_version(snapshot_id)
                table = recent.table if recent is not None else self._require_table(snapshot_id)
                pinned.description = recent
                pinned.table = table
            table = pinned.table
        return table

    def _recent_read_version(self, snapshot_id: SnapshotId) -> _ReadVersion | None:
        recent = self._read_version
        return recent if recent is not None and recent.manifest.snapshot_id == snapshot_id else None

    def _describe_read_version(self, table: _ReadSource) -> _ReadVersion:
        with self.pin_snapshot(SnapshotId(table.version() + 1)):
            return self._describe_pinned_read_version(table)

    def _describe_pinned_read_version(self, table: _ReadSource) -> _ReadVersion:
        """Reuse immutable metadata for the head or for a live snapshot pin."""
        snapshot_id = SnapshotId(table.version() + 1)
        recent = self._recent_read_version(snapshot_id)
        if recent is not None:
            return recent
        with self._read_pins_lock:
            pin = self._read_pins.get(snapshot_id)
        # A writer can pass its own DeltaTable here. Only describe and reuse the
        # reader-owned table under this pin.
        pinned = pin if pin is not None and pin.table is table else None
        if pinned is not None and pinned.description is not None:
            return pinned.description
        with self._read_version_lock:
            if pinned is not None and pinned.description is not None:
                return pinned.description
            cached = self._read_version
            if cached is not None and cached.manifest.snapshot_id == snapshot_id:
                if pinned is not None:
                    pinned.description = cached
                return cached
            metadata_file = self._graph_metadata_file(table) if self._retain_read_version else None
            if (
                cached is not None
                and cached.manifest.snapshot_id.value < snapshot_id.value
                and metadata_file is not None
                and metadata_file == cached.metadata_file
            ):
                head, catalog = cached.head, cached.catalog
            else:
                head, catalog = _metadata(table)
            entry = _ReadVersion(
                table, self._manifest(table, head, catalog), catalog, head, metadata_file
            )
            if pinned is not None:
                pinned.description = entry
            if self._retain_read_version and (
                cached is None or cached.manifest.snapshot_id.value < snapshot_id.value
            ):
                self._read_version = entry
            return entry

    @staticmethod
    def _graph_metadata_file(table: _ReadSource) -> tuple[str, str | None, str] | None:
        if isinstance(table, DeltaReadView):
            return table.metadata_file()
        # Reader features can change the visible rows without replacing a Parquet file.
        if table.protocol().min_reader_version > 2:
            return None
        try:
            files = table.file_uris(file_pruning_predicate=[("kind", "=", "metadata")])
        except (DeltaError, OSError, ValueError):
            return None
        if len(files) != 1:
            return None
        return files[0], table.metadata().description, table.schema().to_json()

    @cached_property
    def _log_storage(self) -> DeltaStorageHandler:
        return DeltaStorageHandler(self._uri, self._options)

    def _commit_info(self, version: int) -> dict[str, Any]:
        commit = self._maybe_commit_info(version)
        if commit is None:
            raise StorageError(f"Missing Delta log file for version {version}.")
        return commit

    def _maybe_commit_info(self, version: int) -> dict[str, Any] | None:
        path = f"_delta_log/{version:020d}.json"
        try:
            probed = self._probed_commit_size
            if probed is not None and probed[0] == version:
                size = probed[1]
            else:
                info = self._log_storage.get_file_info([path])[0]
                if info.type == pafs.FileType.NotFound:
                    return None
                if info.type != pafs.FileType.File:
                    raise StorageError(f"Invalid Delta log file for version {version}.")
                size = info.size
            storage = DeltaStorageHandler(self._uri, self._options, known_sizes={path: size})
            with storage.open_input_file(path) as handle:
                for line in handle.read().splitlines():
                    action = orjson.loads(line)
                    if isinstance(action, dict) and isinstance(
                        commit := action.get("commitInfo"), dict
                    ):
                        return commit
        except (DeltaError, OSError, pa.ArrowException, ValueError) as exc:
            raise StorageError(f"Could not read Delta commit {version}.") from exc
        raise StorageError(f"Missing commit information for Delta version {version}.")

    def _manifest(
        self, table: _ReadSource, head: _Head, catalog: VersionedCatalog
    ) -> SnapshotManifest:
        version = table.version()
        if head.metadata_version != version:
            commit = self._commit_info(version)
            timestamp = commit.get("timestamp")
            if not isinstance(commit.get(_POINT_COMMIT_ID), str) or type(timestamp) is not int:
                raise StorageError(f"Unexpected Delta commit without graph metadata at {version}.")
            created_at = datetime.fromtimestamp(timestamp / 1000, UTC).isoformat(
                timespec="milliseconds"
            )
            head = replace(head, created_at=created_at, receipt=None)
        return head.manifest(SnapshotId(version + 1), catalog.version)

    def capture_snapshot(self) -> SnapshotManifest:
        with self.borrow_current_snapshot() as snapshot:
            return snapshot

    def snapshot_open_guard(self) -> AbstractContextManager[None]:
        """Delta snapshot leases already protect materialization during retention."""
        return nullcontext()

    @contextmanager
    def borrow_current_snapshot(self) -> Iterator[SnapshotManifest]:
        """Hold the selected snapshot from its metadata read through the caller's work."""
        if self._retain_read_version:
            started = monotonic_ns()
            with ExitStack() as pin:
                snapshot: SnapshotManifest | None = None
                while snapshot is None:
                    cached = self._read_version
                    observation = self._read_refresh_observation
                    if cached is not None and (
                        # A refresh started after this borrow can be its freshness check.
                        (
                            observation is not None
                            and observation[0] is cached.manifest
                            and observation[1] > started
                        )
                        or self._cached_snapshot_is_current(cached.manifest.snapshot_id)
                    ):
                        pin.enter_context(self.pin_snapshot(cached.manifest.snapshot_id))
                        snapshot = cached.manifest
                        continue
                    with self._read_refresh_lock:
                        if self._read_version is not cached:
                            # Reuse an overlapping refresh or recheck if it started earlier.
                            continue
                        opened_at = monotonic_ns()
                        table = self._refresh_read_source(cached)
                        if table is None:
                            break
                        snapshot = pin.enter_context(self._borrow_open_snapshot(table))
                        self._read_refresh_observation = (snapshot, opened_at)
                if snapshot is not None:
                    yield snapshot
                    return
        # Initialization can acquire the writer lock. Do it outside the refresh lock.
        with self._borrow_bootstrapped_snapshot() as snapshot:
            yield snapshot

    def _refresh_read_source(self, cached: _ReadVersion | None) -> _ReadSource | None:
        """Refresh under the read refresh lock and publish a detached view."""
        if not self._incremental_refresh:
            return self._try_open()
        previous = self._read_refresh_cursor
        self._read_refresh_cursor = None
        cursor = previous if previous is not None else self._try_open()
        if cursor is None:
            return None
        try:
            if previous is not None:
                cursor.update_incremental()
            # A stale probe may have found a commit beyond the cursor. Reopen
            # if replay failed to advance to a new view of the head.
            if (
                previous is not None
                and cached is not None
                and cursor.version() + 1 <= cached.manifest.snapshot_id.value
            ):
                return self._try_open()
            detached = DeltaReadView.capture(cursor, self._uri, self._options)
        except (DeltaError, OSError, pa.ArrowException):
            return self._try_open()
        if detached is None:
            return cursor if previous is None else self._try_open()
        self._read_refresh_cursor = cursor
        return detached

    def _cached_snapshot_is_current(self, snapshot_id: SnapshotId) -> bool:
        if isinstance(self._layout.delta_dir, Path):
            return self._probe_next_commit(snapshot_id)
        with self._head_probe_lock:
            queued = self._queued_head_probes.get(snapshot_id)
            if queued is None:
                probe: Future[bool] = Future()
                self._queued_head_probes[snapshot_id] = probe
            else:
                probe = queued
        if queued is None:
            try:
                sleep(_HEAD_PROBE_BATCH_SECONDS)
                # Seal the group before issuing the storage request. A
                # later reader must observe any commit made before it arrived.
                with self._head_probe_lock:
                    del self._queued_head_probes[snapshot_id]
                probe.set_result(self._probe_next_commit(snapshot_id))
            except BaseException as exc:
                with self._head_probe_lock:
                    if self._queued_head_probes.get(snapshot_id) is probe:
                        del self._queued_head_probes[snapshot_id]
                probe.set_exception(exc)
                raise
        return probe.result()

    def _probe_next_commit(self, snapshot_id: SnapshotId) -> bool:
        # GraphArrow retains the contiguous commit log, including expired snapshots.
        path = f"_delta_log/{snapshot_id.value:020d}.json"
        try:
            if isinstance(self._layout.delta_dir, Path):
                (self._layout.delta_dir / path).stat()
                return False
            info = self._log_storage.get_file_info([path])[0]
            if info.type == pafs.FileType.File and info.size is not None:
                # A published JSON commit is immutable. Retain only the last size observed.
                self._probed_commit_size = (snapshot_id.value, info.size)
            return bool(info.type == pafs.FileType.NotFound)
        except FileNotFoundError:
            return True
        except (DeltaError, OSError, pa.ArrowException):
            # Let the standard Delta open resolve other storage errors.
            return False

    def bootstrap_snapshot(self) -> SnapshotManifest:
        with self._borrow_bootstrapped_snapshot() as snapshot:
            return snapshot

    @contextmanager
    def _borrow_bootstrapped_snapshot(self) -> Iterator[SnapshotManifest]:
        with self._borrow_open_snapshot(self._open_or_create()) as snapshot:
            yield snapshot

    @contextmanager
    def _borrow_open_snapshot(
        self, source: _ReadSource | SnapshotManifest
    ) -> Iterator[SnapshotManifest]:
        if isinstance(source, SnapshotManifest):
            with self.pin_snapshot(source.snapshot_id):
                yield source
            return
        snapshot_id = SnapshotId(source.version() + 1)
        with self._pin_read_snapshot(snapshot_id, source):
            yield self._describe_pinned_read_version(self._read_table(snapshot_id)).manifest

    def _open_or_create(self) -> DeltaTable | SnapshotManifest:
        table = self._try_open()
        if table is not None:
            return table
        with self._writer.hold():
            table = self._try_open()
            if table is not None:
                return table
            self._check_native_layout()
            catalog = VersionedCatalog.empty(version=CatalogVersion(1))
            head = _new_head(
                0,
                0,
                {},
                {},
                None,
                self._configured_range_size(_DEFAULT_EDGE_RANGE_SIZE),
                metadata_version=0,
                node_ranges=True,
            )
            data = _metadata_table(head, catalog, managed=self._manage_new_table)
            configuration = {
                "delta.isolationLevel": "Serializable",
                "delta.enableExpiredLogCleanup": "false",
            }
            if self._checkpoint_interval is not None:
                configuration["delta.checkpointInterval"] = str(self._checkpoint_interval)
            if self._manage_new_table:
                self._retention.initialize()
            try:
                write_deltalake(
                    self._uri,
                    data,
                    partition_by=["kind"],
                    configuration=configuration,
                    description=MANAGED_DESCRIPTION if self._manage_new_table else None,
                    storage_options=self._options,
                    commit_properties=CommitProperties(max_commit_retries=0),
                )
            except (DeltaError, OSError) as exc:
                table = self._try_open()
                if table is None:
                    raise StorageError("Could not initialize the Delta graph.") from exc
                return table
            self._managed = self._manage_new_table
            return head.manifest(SnapshotId(1), catalog.version)

    def _check_native_layout(self) -> None:
        paths = [
            self.layout.current_snapshot_path,
            self.layout.current_catalog_path,
        ]
        # Use the same provider and credentials as Delta itself, including when
        # the application has no provider SDK or PyArrow cloud credentials.
        try:
            if isinstance(self.layout.root, Path):
                entries = pafs.LocalFileSystem().get_file_info([str(path) for path in paths])
            else:
                storage = DeltaStorageHandler(self.layout.root, self._options)
                entries = storage.get_file_info([self.layout.relative_path(path) for path in paths])
        except (DeltaError, OSError) as exc:
            raise StorageError("Could not check the database path for Native metadata.") from exc
        if any(entry.type != pafs.FileType.NotFound for entry in entries):
            raise StorageError("This database uses the Native backend. Choose a new Delta path.")

    def current_exists(self) -> bool:
        return self._try_open() is not None

    def load_manifest(self, snapshot_id: SnapshotId | None = None) -> SnapshotManifest:
        table = self._require_table(None) if snapshot_id is None else self._read_table(snapshot_id)
        return self._describe_read_version(table).manifest

    def try_load_current_pointer(self) -> CurrentSnapshotPointer | None:
        table = self._try_open()
        return _pointer(self._describe_read_version(table).manifest) if table is not None else None

    def load_current_pointer(self) -> CurrentSnapshotPointer:
        return _pointer(self.load_manifest())

    def load_at(self, pointer: CurrentSnapshotPointer) -> SnapshotManifest:
        manifest = self.load_manifest(pointer.snapshot_id)
        if pointer != _pointer(manifest):
            raise StorageError("The Delta snapshot pointer does not match the log.")
        return manifest

    def history(
        self, pointer: CurrentSnapshotPointer | None = None
    ) -> Iterator[tuple[str, SnapshotManifest]]:
        head = pointer or self.load_current_pointer()
        if self._managed is None:
            self._require_table(head.snapshot_id)
        minimum = self._retention.minimum_snapshot_id() if self._managed else 1
        if head.snapshot_id.value < minimum:
            # Report the same error as an explicit load of an expired snapshot.
            self.load_manifest(head.snapshot_id)
        with self.pin_snapshot(SnapshotId(minimum)):
            for value in range(head.snapshot_id.value, minimum - 1, -1):
                manifest = self.load_manifest(SnapshotId(value))
                yield _pointer(manifest).manifest_path, manifest

    def publish(
        self, manifest: SnapshotManifest, *, base: SnapshotManifest | None = None
    ) -> CurrentSnapshotPointer:
        raise StorageError("Publish Delta snapshots through graph transactions.")

    def idempotency_base(
        self, pointer: CurrentSnapshotPointer, key_hash: str
    ) -> tuple[SnapshotManifest, IdempotencyReceipt | None]:
        with self.pin_snapshot(pointer.snapshot_id):
            table = self._read_table(pointer.snapshot_id)
            manifest = self._describe_read_version(table).manifest
            if pointer != _pointer(manifest):
                raise StorageError("The Delta snapshot pointer does not match the log.")
            return manifest, _find_receipt(table, key_hash)

    def current_idempotency_base(
        self, key_hash: str
    ) -> tuple[SnapshotManifest, IdempotencyReceipt | None]:
        """Reuse the freshness check and read the receipt from the pinned write base."""
        with self.borrow_current_snapshot() as snapshot:
            table = self._read_table(snapshot.snapshot_id)
            scope = self._writer.scope
            if scope is None:
                return snapshot, _find_receipt(table, key_hash)
            previous = scope.receipt_bucket
            scope.receipt_bucket = None
            # The recheck following a conflict can only replay or raise.
            if previous is not None and previous.key_hash == key_hash:
                return snapshot, _find_receipt(table, key_hash)
            kind = _receipt_partition(key_hash)
            buckets = self._describe_pinned_read_version(table).receipt_buckets
            if buckets is None:
                return snapshot, _find_receipt(table, key_hash)
            count, size = buckets.get(kind, (0, 0))
            if count > _RECEIPT_REUSE_MAX_ROWS or size > _RECEIPT_REUSE_MAX_PARQUET_BYTES:
                return snapshot, _find_receipt(table, key_hash)
            rows = _partition(table, kind, _RECEIPT_COLUMNS)
            receipt = _receipt_from_rows(rows.filter(pc.equal(rows["key_hash"], key_hash)))
            if receipt is None:
                scope.receipt_bucket = _ReceiptBucket(snapshot.snapshot_id, key_hash, rows)
            return snapshot, receipt

    def _append_write_receipt(
        self, table: DeltaTable, kind: str, receipt: IdempotencyReceipt
    ) -> pa.Table:
        scope = self._writer.scope
        bucket = scope.receipt_bucket if scope is not None else None
        if (
            bucket is not None
            and bucket.snapshot_id.value == table.version() + 1
            and bucket.key_hash == receipt.key_hash
        ):
            return _append_receipt(table, kind, receipt, stored=bucket.rows)
        return _append_receipt(table, kind, receipt)

    @writer_locked
    def write(self, catalog: VersionedCatalog, *, base: SnapshotManifest | None = None) -> str:
        current = base or self.bootstrap_snapshot()
        with self.pin_snapshot(current.snapshot_id):
            table = self._require_table(current.snapshot_id)
            head, old_catalog = _metadata(table)
            if catalog.version.value <= old_catalog.version.value:
                if catalog == old_catalog:
                    return f"delta/{current.snapshot_id.value}/catalog"
                raise WriteConflictError("Catalog changed. Reload it before writing a new version.")
            candidate = _new_head(
                head.node_count,
                head.edge_count,
                head.labels,
                head.relationships,
                None,
                head.edge_range_size,
                metadata_version=table.version() + 1,
                node_ranges=head.node_ranges,
            )
            snapshot = self._commit(table, candidate, catalog, {})
            return f"delta/{snapshot.snapshot_id.value}/catalog"

    def load_for_snapshot(self, manifest: SnapshotManifest) -> VersionedCatalog:
        catalog = self._describe_read_version(self._read_table(manifest.snapshot_id)).catalog
        if catalog.version != manifest.catalog_version:
            raise CatalogError("Catalog version does not match the Delta snapshot.")
        # Index options may contain mutable objects exposed by the catalog API.
        return deepcopy(catalog) if catalog.indexes else catalog

    def load(self, version: CatalogVersion | None = None) -> VersionedCatalog:
        if version is None:
            catalog = self._describe_read_version(self._require_table(None)).catalog
            return deepcopy(catalog) if catalog.indexes else catalog
        for _, manifest in self.history():
            if manifest.catalog_version == version:
                return self.load_for_snapshot(manifest)
            if manifest.catalog_version.value < version.value:
                break
        raise CatalogError(f"Catalog version does not exist: {version.value}")

    @writer_locked
    def publish_graph(
        self,
        *,
        previous_state: GraphState,
        catalog: VersionedCatalog,
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
        edge_table: pa.Table | None = None,
        idempotency_receipt: IdempotencyReceipt | None = None,
    ) -> GraphState:
        from grapharrow.use_cases.graph import GraphState

        with self.pin_snapshot(previous_state.snapshot.snapshot_id):
            base = self._require_table(previous_state.snapshot.snapshot_id)
            previous_head = self._describe_read_version(
                self._read_table(previous_state.snapshot.snapshot_id)
            ).head
            partitions: dict[str, pa.Table] = {}
            node_records = tuple(nodes)
            edge_records = tuple(edges)
            node_table = previous_state.node_table
            replace_all_nodes = False
            if node_records != previous_state.nodes:
                node_table = _nodes_table(nodes)
                replace_all_nodes = not previous_head.node_ranges
                if replace_all_nodes:
                    partitions.update(_node_partitions(node_table))
                else:
                    partitions.update(
                        _changed_partitions(
                            _node_partitions(node_table),
                            _node_partitions(previous_state.node_table),
                            NODE_SCHEMA,
                        )
                    )
            if edge_records != previous_state.edges:
                edge_table = edge_table if edge_table is not None else _edges_table(edges)
                partitions.update(
                    _changed_edge_partitions(
                        edge_table, previous_state.edge_table, previous_head.edge_range_size
                    )
                )
            else:
                edge_table = previous_state.edge_table
            if idempotency_receipt is not None:
                kind = _receipt_partition(idempotency_receipt.key_hash)
                partitions[kind] = self._append_write_receipt(base, kind, idempotency_receipt)

            head = _new_head(
                len(nodes),
                len(edges),
                dict(Counter(label for node in nodes for label in set(node.labels))),
                dict(Counter(edge.rel_type for edge in edges)),
                idempotency_receipt,
                previous_head.edge_range_size,
                metadata_version=base.version() + 1,
                node_ranges=previous_head.node_ranges or replace_all_nodes,
            )
            manifest = self._commit(
                base, head, catalog, partitions, replace_all_nodes=replace_all_nodes
            )
            return GraphState(catalog, manifest, node_table, edge_table, node_records, edge_records)

    def _commit(
        self,
        base: DeltaTable,
        head: _Head,
        catalog: VersionedCatalog,
        partitions: dict[str, pa.Table],
        *,
        replace_all_edges: bool = False,
        replace_all_nodes: bool = False,
    ) -> SnapshotManifest:
        snapshot_id = SnapshotId(head.metadata_version + 1)
        data = pa.concat_tables(
            [
                _metadata_table(head, catalog, managed=bool(self._managed)),
                *(_rows(part, kind) for kind, part in partitions.items()),
            ]
        )
        kinds = ", ".join(
            f"'{kind}'"
            for kind in ("metadata", *partitions)
            if (not replace_all_edges or not kind.startswith("edges"))
            and (not replace_all_nodes or not kind.startswith("nodes"))
        )
        predicate = f"kind IN ({kinds})"
        if replace_all_edges:
            predicate += f" OR ({_EDGE_PARTITION_PREDICATE})"
        if replace_all_nodes:
            predicate += f" OR ({_NODE_PARTITION_PREDICATE})"
        try:
            # The Python writer reloads the table URI when passed a string. Pass the
            # pinned table, and disallow rebasing a graph mutation on a newer version.
            write_deltalake(
                base,
                data,
                mode="overwrite",
                predicate=predicate,
                commit_properties=CommitProperties(max_commit_retries=0),
            )
        except (DeltaError, OSError) as exc:
            # A failed response can follow successful creation of the log entry.
            # The candidate has exactly one possible version and a unique commit ID.
            try:
                latest = self._require_table(None)
                if latest.version() >= snapshot_id.value - 1:
                    candidate, _ = _metadata(self._require_table(snapshot_id))
                    if candidate.commit_id == head.commit_id:
                        return head.manifest(snapshot_id, catalog.version)
                    raise WriteConflictError(_CONFLICT) from exc
            except (StorageError, CatalogError, pa.ArrowException):
                raise CommitOutcomeUnknownError(
                    "Could not confirm the Delta commit outcome."
                ) from exc
            if isinstance(exc, CommitFailedError):
                raise WriteConflictError(_CONFLICT) from exc
            raise CommitOutcomeUnknownError("Could not confirm the Delta commit outcome.") from exc
        return head.manifest(snapshot_id, catalog.version)

    @contextmanager
    def _borrow_point_snapshot(
        self, receipt: PointWriteReceipt | None
    ) -> Iterator[SnapshotManifest]:
        if receipt is None:
            with self.borrow_current_snapshot() as snapshot:
                yield snapshot
        else:
            with self.pin_snapshot(receipt.base.snapshot_id):
                yield receipt.base

    @contextmanager
    def edit_edge_properties_by_id(
        self,
        *,
        edge_id: int,
        rel_type: str | None,
        properties: dict[str, object],
        receipt: PointWriteReceipt | None = None,
    ) -> Generator[tuple[EdgeRecord | None] | None, None, None]:
        """Replace one canonical range after the caller builds its result successfully."""
        with self._writer.hold(), self._borrow_point_snapshot(receipt) as snapshot:
            current = self._describe_pinned_read_version(self._read_table(snapshot.snapshot_id))
            width = current.head.edge_range_size
            if not width or current.catalog.constraints:
                yield None
                return
            kind = _edge_partition((edge_id - 1) // width)
            table = _partition(current.table, kind, EDGE_SCHEMA.names).cast(EDGE_SCHEMA)
            positions = pc.indices_nonzero(pc.equal(table["edge_id"], edge_id))
            if not len(positions):
                yield (None,) if receipt is None else None
                return
            if len(positions) != 1:
                yield None
                return
            position = positions[0].as_py()
            before = self._graph.edge_records_from_table(table.slice(position, 1))[0]
            if (rel_type is not None and before.rel_type != rel_type) or not _can_patch_properties(
                before, properties
            ):
                yield None
                return
            after = replace(before, properties={**before.properties, **properties})
            yield (after,)
            if before == after:
                if receipt is not None:
                    self._commit_receipt(snapshot.snapshot_id, receipt.prepare())
                return
            self._commit_point(
                snapshot.snapshot_id, current.head.commit_id, kind, after, receipt, range_rows=table
            )

    @contextmanager
    def edit_edge_property_batch(
        self,
        *,
        property_name: str,
        updates: dict[int, object],
        rel_type: str | None,
        receipt: PointWriteReceipt | None = None,
    ) -> Generator[tuple[EdgeRecord, ...] | None, None, None]:
        """Stage every touched range once and publish the complete batch atomically."""
        with self._writer.hold(), self._borrow_point_snapshot(receipt) as snapshot:
            current = self._describe_pinned_read_version(self._read_table(snapshot.snapshot_id))
            width = current.head.edge_range_size
            if not width or current.catalog.constraints or not updates:
                yield None
                return
            ranges: dict[str, list[int]] = {}
            for edge_id in updates:
                ranges.setdefault(_edge_partition((edge_id - 1) // width), []).append(edge_id)
            staged = {}
            results = {}
            for kind, ids in ranges.items():
                table = _partition(current.table, kind, EDGE_SCHEMA.names).cast(EDGE_SCHEMA)
                positions = pc.index_in(pa.array(ids, type=pa.int64()), value_set=table["edge_id"])
                if positions.null_count:
                    yield None
                    return
                changed: dict[int, str] = {}
                for edge in self._graph.edge_records_from_table(table.take(positions)):
                    properties = {property_name: updates[edge.edge_id]}
                    if (rel_type is not None and edge.rel_type != rel_type) or (
                        not _can_patch_properties(edge, properties)
                    ):
                        yield None
                        return
                    after = replace(edge, properties={**edge.properties, **properties})
                    results[edge.edge_id] = after
                    if after != edge:
                        changed[edge.edge_id] = property_codec.dumps(after.properties)
                if changed:
                    matches = pc.index_in(
                        table["edge_id"], value_set=pa.array(list(changed), type=pa.int64())
                    )
                    properties_json = pc.coalesce(
                        pc.take(pa.array(list(changed.values())), matches), table["properties_json"]
                    )
                    staged[kind] = table.set_column(
                        table.schema.get_field_index("properties_json"),
                        table.schema.field("properties_json"),
                        properties_json,
                    )
            yield tuple(results[edge_id] for edge_id in updates)
            prepared = receipt.prepare() if receipt is not None else None
            if not staged:
                if prepared is not None:
                    self._commit_receipt(snapshot.snapshot_id, prepared)
                return
            # Match the general batch writer's pinned-base policy, including
            # ranges read for the result whose rows did not change.
            with self._point_publication(snapshot.snapshot_id, max_retries=0) as (base, commit):
                if prepared is not None:
                    kind = _receipt_partition(prepared.key_hash)
                    staged[kind] = self._append_write_receipt(base, kind, prepared)
                kinds = ", ".join(f"'{kind}'" for kind in staged)
                write_deltalake(
                    base,
                    pa.concat_tables([_rows(table, kind) for kind, table in staged.items()]),
                    mode="overwrite",
                    predicate=f"kind IN ({kinds})",
                    commit_properties=commit,
                )

    @contextmanager
    def edit_node_properties_by_id(
        self,
        *,
        node_id: int,
        labels: tuple[str, ...],
        properties: dict[str, object],
        receipt: PointWriteReceipt | None = None,
    ) -> Generator[tuple[NodeRecord | None] | None, None, None]:
        """Read a node by ID from its pinned range and publish through Delta OCC."""
        with self._writer.hold(), self._borrow_point_snapshot(receipt) as snapshot:
            current = self._describe_pinned_read_version(self._read_table(snapshot.snapshot_id))
            if not current.head.node_ranges or current.catalog.constraints:
                yield None
                return
            kind = _node_partition((node_id - 1) // NODE_RANGE_SIZE)
            table = _partition(
                current.table, kind, NODE_SCHEMA.names, ds.field("node_id") == node_id, exact=True
            )
            if not table.num_rows:
                yield (None,) if receipt is None else None
                return
            if table.num_rows != 1:
                yield None
                return
            before = node_records_from_table(table)[0]
            if not set(labels).issubset(before.labels):
                yield (None,) if receipt is None else None
                return
            if not _can_patch_properties(before, properties):
                yield None
                return
            after = replace(before, properties={**before.properties, **properties})
            yield (after,)
            if before != after:
                self._commit_point(
                    snapshot.snapshot_id, current.head.commit_id, kind, after, receipt
                )
            elif receipt is not None:
                self._commit_receipt(snapshot.snapshot_id, receipt.prepare())

    def _take_point_write_table(self, snapshot_id: SnapshotId) -> DeltaTable:
        """Take a writer-owned handle for exactly the pinned transaction version."""
        table = self._point_write_table
        self._point_write_table = None
        if table is not None:
            expected = snapshot_id.value - 1
            try:
                if table.version() < expected:
                    table.update_incremental()
                if table.version() == expected:
                    return table
            except (DeltaError, OSError):
                pass
        return self._require_table(snapshot_id)

    def _commit_point(
        self,
        snapshot_id: SnapshotId,
        metadata_id: str,
        kind: str,
        entity: EdgeRecord | NodeRecord,
        receipt: PointWriteReceipt | None,
        *,
        range_rows: pa.Table | None = None,
    ) -> None:
        prepared = receipt.prepare() if receipt is not None else None
        field, identifier = (
            ("node_id", entity.node_id)
            if isinstance(entity, NodeRecord)
            else ("edge_id", entity.edge_id)
        )
        # The guard matches no row in the base and matches replacement metadata during rebase.
        guard = f"(kind = 'metadata' AND (key_hash IS NULL OR key_hash <> '{metadata_id}'))"
        properties = property_codec.dumps(entity.properties)
        with self._point_publication(snapshot_id, max_retries=_POINT_COMMIT_RETRIES) as (
            base,
            commit,
        ):
            if prepared is None:
                metrics = base.update(
                    predicate=f"(kind = '{kind}' AND {field} = {identifier}) OR {guard}",
                    new_values={"properties_json": properties},
                    commit_properties=commit,
                )
                if metrics.get("num_updated_rows") != 1:
                    raise CommitOutcomeUnknownError(
                        "Delta did not report exactly one updated entity."
                    )
            else:
                if range_rows is None:
                    schema = NODE_SCHEMA if field == "node_id" else EDGE_SCHEMA
                    range_rows = _partition(base, kind, schema.names).cast(schema)
                self._commit_point_with_receipt(
                    base, kind, range_rows, field, identifier, properties, guard, prepared, commit
                )

    def _commit_receipt(self, snapshot_id: SnapshotId, receipt: IdempotencyReceipt) -> None:
        # The response depends on the matched row even though this write does not replace it.
        # Reject a new head so a changed range or catalog cannot invalidate the response.
        with self._point_publication(snapshot_id, max_retries=0) as (base, commit):
            kind = _receipt_partition(receipt.key_hash)
            write_deltalake(
                base,
                _rows(self._append_write_receipt(base, kind, receipt), kind),
                mode="overwrite",
                predicate=f"kind = '{kind}'",
                commit_properties=commit,
            )

    @contextmanager
    def _point_publication(
        self, snapshot_id: SnapshotId, *, max_retries: int
    ) -> Iterator[tuple[DeltaTable, CommitProperties]]:
        base = self._take_point_write_table(snapshot_id)
        base_version = base.version()
        commit_id = uuid4().hex
        commit = CommitProperties(
            custom_metadata={_POINT_COMMIT_ID: commit_id}, max_commit_retries=max_retries
        )
        try:
            yield base, commit
        except (DeltaError, OSError) as exc:
            # A successful rebase can publish at any version after the original base.
            # A retried conditional put can report a conflict with its own commit.
            try:
                if self._find_point_commit(base_version, commit_id):
                    return
            except StorageError as read_error:
                raise CommitOutcomeUnknownError(
                    "Could not confirm the Delta commit outcome."
                ) from read_error
            if isinstance(exc, CommitFailedError):
                raise WriteConflictError(_CONFLICT) from exc
            raise CommitOutcomeUnknownError("Could not confirm the Delta commit outcome.") from exc
        self._point_write_table = base

    def _find_point_commit(self, base_version: int, commit_id: str) -> bool:
        # GraphArrow keeps the contiguous JSON log, including expired snapshots.
        end = base_version + _POINT_RECOVERY_LOG_PROBES
        for version in range(base_version + 1, end + 1):
            entry = self._maybe_commit_info(version)
            if entry is None:
                return False
            if entry.get(_POINT_COMMIT_ID) == commit_id:
                return True
        # Fall back to finding the latest version when the transaction's base is far behind.
        latest = self._require_table(None).version()
        return any(
            self._commit_info(version).get(_POINT_COMMIT_ID) == commit_id
            for version in range(latest, end, -1)
        )

    def _commit_point_with_receipt(
        self,
        base: DeltaTable,
        kind: str,
        rows: pa.Table,
        field: str,
        identifier: int,
        properties: str,
        guard: str,
        receipt: IdempotencyReceipt,
        commit: CommitProperties,
    ) -> None:
        updated = rows.set_column(
            rows.schema.get_field_index("properties_json"),
            rows.schema.field("properties_json"),
            pc.if_else(pc.equal(rows[field], identifier), properties, rows["properties_json"]),
        )
        receipts = _receipt_partition(receipt.key_hash)
        # Overwrite both partitions in one transaction. A competing receipt in the same
        # bucket, including an initially empty bucket, prevents a stale replace and replay.
        data = pa.concat_tables(
            [
                _rows(updated, kind),
                _rows(self._append_write_receipt(base, receipts, receipt), receipts),
            ]
        )
        write_deltalake(
            base,
            data,
            mode="overwrite",
            predicate=f"kind IN ('{kind}', '{receipts}') OR {guard}",
            commit_properties=commit,
        )

    def initialize(self) -> InitializationResult:
        snapshot = self.bootstrap_snapshot()
        return InitializationResult(self.load_for_snapshot(snapshot), snapshot)

    def _verify_rows(self, manifest: SnapshotManifest) -> None:
        table = self._require_table(manifest.snapshot_id)
        for kind, files, schema in (
            ("nodes", manifest.nodes, NODE_SCHEMA),
            ("edges", manifest.edges, EDGE_SCHEMA),
        ):
            rows = _partition(table, kind, schema.names)
            if rows.num_rows != sum(file.row_count for file in files):
                raise StorageError(f"Delta {kind} row count does not match the snapshot.")

    def verify(self, *, verify_checksums: bool = False) -> VerificationReport:
        manifest = None
        notes = (
            ("Delta verification reads the active Parquet data without per-file SHA-256 hashes.",)
            if verify_checksums
            else ()
        )
        try:
            table = self._require_table(None)
            with self.pin_snapshot(SnapshotId(table.version() + 1)):
                head, catalog = _metadata(table)
                manifest = self._manifest(table, head, catalog)
                self._verify_rows(manifest)
        except StorageError as exc:
            return VerificationReport(
                ok=False,
                catalog_version=manifest.catalog_version if manifest is not None else None,
                snapshot_pointer=_pointer(manifest) if manifest is not None else None,
                warnings=(*notes, str(exc)),
            )
        return VerificationReport(
            True, manifest.catalog_version, _pointer(manifest), warnings=notes
        )

    def recover(
        self,
        *,
        dry_run: bool = True,
        cleanup_orphan_staged: bool = True,
        verify_checksums: bool = False,
    ) -> RecoveryReport:
        return RecoveryReport(
            verification=self.verify(verify_checksums=verify_checksums),
            warnings=("Delta recovery leaves unreferenced objects in place.",),
        )

    @writer_locked
    def compact(self) -> SnapshotManifest:
        base = self._require_table(None)
        with self.pin_snapshot(SnapshotId(base.version() + 1)):
            head, catalog = _metadata(base)
            manifest = head.manifest(SnapshotId(base.version() + 1), catalog.version)
            width = self._configured_range_size(head.edge_range_size)
            candidate = _new_head(
                head.node_count,
                head.edge_count,
                head.labels,
                head.relationships,
                None,
                width,
                metadata_version=base.version() + 1,
                node_ranges=True,
            )
            partitions = _edge_partitions(self._graph.load_edge_table(manifest.edges), width)
            partitions.update(_node_partitions(self._graph.load_node_table(manifest.nodes)))
            return self._commit(
                base,
                candidate,
                catalog,
                partitions,
                replace_all_edges=True,
                replace_all_nodes=True,
            )

    def _configured_range_size(self, default: int) -> int:
        if self._edge_range_size is not None:
            return self._edge_range_size
        return default

    def vacuum(self) -> tuple[str, ...]:
        raise StorageError("Delta vacuum is unavailable while historical snapshots are retained.")

    def storage_inventory(self) -> DeltaStorageInventory:
        table = self._require_table(None)
        inventory = inspect_delta_storage(
            table, self._filesystem, self._layout.delta_dir, self._options
        )
        return (
            replace(inventory, retention=self._retention.status()) if self._managed else inventory
        )

    def apply_retention(
        self, *, minimum_snapshot_id: int, online: bool = False
    ) -> DeltaRetentionResult:
        table = self._require_table(None)
        if not self._managed:
            raise StorageError(
                "Applying retention requires a Delta database with managed retention."
            )
        if not 1 <= minimum_snapshot_id <= table.version() + 1:
            raise ValueError("minimum_snapshot_id must identify a published snapshot.")
        plan = plan_delta_retention(
            table,
            self._filesystem,
            self._layout.delta_dir,
            self._options,
            keep_last=1,
            min_age_hours=0,
            protect_from_snapshot_id=minimum_snapshot_id,
            minimum_snapshot_id=self._retention.minimum_snapshot_id(),
        )
        token = self._retention.begin_cleanup(minimum_snapshot_id, online=online)
        removed = []
        for file in plan.candidate_files:
            try:
                self._filesystem.delete_file(self._layout.delta_retention_file(file.path))
            except FileNotFoundError:
                continue
            removed.append(file.path)
        self._retention.finish_cleanup(token)
        return DeltaRetentionResult(minimum_snapshot_id, tuple(removed))

    def recover_retention_pins(
        self, incarnation_id: str, *, verifier: Path | None = None
    ) -> DeltaPinRecoveryResult:
        table = self._require_table(None)
        if not self._managed:
            raise StorageError("Recovering pins requires a Delta database with managed retention.")
        return self._retention.recover_pins(
            incarnation_id, database_id=str(table.metadata().id), verifier=verifier
        )

    def plan_retention(
        self,
        *,
        keep_last: int,
        min_age_hours: int = 0,
        protect_from_snapshot_id: int | None = None,
    ) -> RetentionPlan:
        if keep_last < 1:
            raise ValueError("keep_last must be at least 1.")
        if min_age_hours < 0:
            raise ValueError("min_age_hours must be nonnegative.")
        table = self._require_table(None)
        return plan_delta_retention(
            table,
            self._filesystem,
            self._layout.delta_dir,
            self._options,
            keep_last=keep_last,
            min_age_hours=min_age_hours,
            protect_from_snapshot_id=protect_from_snapshot_id,
            minimum_snapshot_id=self._retention.minimum_snapshot_id() if self._managed else 1,
        )


def _new_head(
    node_count: int,
    edge_count: int,
    labels: dict[str, int],
    relationships: dict[str, int],
    receipt: IdempotencyReceipt | None,
    edge_range_size: int,
    *,
    metadata_version: int,
    node_ranges: bool = False,
) -> _Head:
    return _Head(
        uuid4().hex,
        datetime.now(UTC).isoformat(timespec="seconds"),
        node_count,
        edge_count,
        labels,
        relationships,
        receipt,
        edge_range_size,
        metadata_version,
        node_ranges,
    )


def _rows(table: pa.Table, kind: str) -> pa.Table:
    size = table.num_rows
    return pa.Table.from_arrays(
        [
            pa.repeat(kind, size)
            if field.name == "kind"
            else table[field.name]
            if field.name in table.column_names
            else pa.nulls(size, type=field.type)
            for field in _SCHEMA
        ],
        schema=_SCHEMA,
    )


def _metadata_table(head: _Head, catalog: VersionedCatalog, *, managed: bool = False) -> pa.Table:
    graph = asdict(head)
    if not head.node_ranges:
        del graph["node_ranges"]
    payload = {
        "format": _MANAGED_FORMAT if managed else _FORMAT,
        "graph": graph,
        "catalog": serialize_catalog(catalog),
    }
    return _rows(
        pa.table(
            {
                "properties_json": [orjson.dumps(payload).decode()],
                "key_hash": [head.commit_id],
            }
        ),
        "metadata",
    )


def _partition(
    table: _ReadSource,
    kind: str,
    columns: list[str],
    expression: object = None,
    *,
    exact: bool = False,
) -> pa.Table:
    try:
        partition_filters = {
            "edges": _EDGE_PARTITION_PREDICATE,
            "nodes": _NODE_PARTITION_PREDICATE,
        }
        partition_filter = partition_filters.get(kind) if not exact else None
        if isinstance(table, DeltaReadView):
            dataset = table.partition_dataset(kind, include_ranges=partition_filter is not None)
        else:
            dataset = table.to_pyarrow_dataset(
                file_pruning_predicate=partition_filter or [("kind", "=", kind)]
            )
        if kind == "metadata" or kind.startswith("receipt-"):
            # Avoid background Parquet work on Delta's Python-backed filesystem
            # when a writer exits immediately after reading metadata or receipts.
            return dataset.to_table(
                columns=columns,
                filter=expression,
                use_threads=False,
                fragment_scan_options=ds.ParquetFragmentScanOptions(pre_buffer=False),
            )
        return dataset.to_table(columns=columns, filter=expression)
    except (DeltaError, OSError, pa.ArrowException) as exc:
        raise StorageError(f"Could not read the Delta {kind} partition.") from exc


def _metadata(table: _ReadSource) -> tuple[_Head, VersionedCatalog]:
    rows = _partition(table, "metadata", ["properties_json", "key_hash"])
    if rows.num_rows != 1:
        raise StorageError("Expected exactly one metadata row at the captured Delta version.")
    try:
        payload = orjson.loads(rows["properties_json"][0].as_py())
        data = payload["graph"]
        description = (
            table.description if isinstance(table, DeltaReadView) else table.metadata().description
        )
        expected_format = _MANAGED_FORMAT if description == MANAGED_DESCRIPTION else _FORMAT
        if payload["format"] != expected_format or "metadata_version" not in data:
            raise StorageError("Unsupported GraphArrow Delta layout. Use a new database path.")
        width = data["edge_range_size"]
        if type(width) is not int or not 0 <= width < 2**63:
            raise ValueError("Invalid edge range size")
        if type(data.get("node_ranges", False)) is not bool:
            raise ValueError("Invalid node range layout")
        version, commit_id = data["metadata_version"], data["commit_id"]
        if (
            type(version) is not int
            or not 0 <= version <= table.version()
            or type(commit_id) is not str
            or len(commit_id) != 32
            or any(char not in "0123456789abcdef" for char in commit_id)
            or rows["key_hash"][0].as_py() != commit_id
        ):
            raise ValueError("Invalid graph metadata guard")
        receipt = data["receipt"]
        if receipt is not None:
            data["receipt"] = IdempotencyReceipt(**receipt)
        return _Head(**data), deserialize_catalog(payload["catalog"])
    except (CatalogError, KeyError, TypeError, ValueError) as exc:
        raise StorageError("Invalid GraphArrow metadata in the Delta log.") from exc


def _pointer(manifest: SnapshotManifest) -> CurrentSnapshotPointer:
    version = manifest.snapshot_id.value - 1
    return CurrentSnapshotPointer(
        manifest.snapshot_id,
        f"delta/_delta_log/{version:020d}.json",
        manifest.commit_txid,
        manifest.created_at,
    )


def _receipt_partition(key_hash: str) -> str:
    return f"receipt-{key_hash[:2]}"


def _find_receipt(table: _ReadSource, key_hash: str) -> IdempotencyReceipt | None:
    rows = _partition(
        table, _receipt_partition(key_hash), _RECEIPT_COLUMNS, ds.field("key_hash") == key_hash
    )
    return _receipt_from_rows(rows)


def _receipt_from_rows(rows: pa.Table) -> IdempotencyReceipt | None:
    if rows.num_rows > 1:
        raise StorageError("Duplicate idempotency receipts in the Delta snapshot.")
    return IdempotencyReceipt(**rows.to_pylist()[0]) if rows.num_rows else None


def _append_receipt(
    table: DeltaTable, kind: str, receipt: IdempotencyReceipt, *, stored: pa.Table | None = None
) -> pa.Table:
    if stored is None:
        stored = _partition(table, kind, _RECEIPT_COLUMNS)
    row = pa.Table.from_pylist(
        [asdict(receipt)], schema=pa.schema([_SCHEMA.field(name) for name in _RECEIPT_COLUMNS])
    )
    return pa.concat_tables([stored.cast(row.schema), row])


def _edge_partition(bucket: int) -> str:
    return f"edges-{bucket:020d}"


def _edge_partitions(table: pa.Table, width: int) -> dict[str, pa.Table]:
    if not width:
        return {"edges": table}
    if not table.num_rows:
        return {}
    return {_edge_partition(bucket): rows for bucket, rows in _edge_ranges(table, width)}


def _node_partitions(table: pa.Table) -> dict[str, pa.Table]:
    if not table.num_rows:
        return {}
    return {
        _node_partition(bucket): rows
        for bucket, rows in _id_ranges(table, "node_id", NODE_RANGE_SIZE)
    }


def _node_partition(bucket: int) -> str:
    # Keep the original partition name for the first range.
    return "nodes" if bucket == 0 else f"nodes-{bucket:020d}"


def _changed_edge_partitions(
    table: pa.Table, previous: pa.Table, width: int
) -> dict[str, pa.Table]:
    if not width:
        return {"edges": table}
    return _changed_partitions(
        _edge_partitions(table, width), _edge_partitions(previous, width), EDGE_SCHEMA
    )


def _changed_partitions(
    current: dict[str, pa.Table], old: dict[str, pa.Table], schema: pa.Schema
) -> dict[str, pa.Table]:
    changed = {
        kind: rows
        for kind, rows in current.items()
        if kind not in old or not rows.equals(old[kind])
    }
    changed.update({kind: schema.empty_table() for kind in old.keys() - current.keys()})
    return changed


def _storage_options(root: str | Path, settings: GraphArrowSettings) -> dict[str, str]:
    if isinstance(root, Path):
        return {}
    scheme = urlsplit(root).scheme
    if scheme in {"s3", "s3a"}:
        if settings.s3_anonymous:
            raise StorageError("The Delta backend requires permission to publish S3 log objects.")
        endpoint = settings.s3_endpoint
        if endpoint and "://" not in endpoint:
            endpoint = f"{settings.s3_scheme or 'https'}://{endpoint}"
        options = {
            "AWS_ACCESS_KEY_ID": settings.s3_access_key,
            "AWS_SECRET_ACCESS_KEY": settings.s3_secret_key,
            "AWS_SESSION_TOKEN": settings.s3_session_token,
            "AWS_REGION": settings.s3_region,
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ALLOW_HTTP": "true" if endpoint and endpoint.startswith("http://") else None,
            "AWS_S3_ADDRESSING_STYLE": "path" if endpoint else None,
            "AWS_CONDITIONAL_PUT": "etag",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "false",
        }
    elif scheme in {"az", "adl", "abfs", "abfss"}:
        endpoint = settings.azure_blob_storage_authority
        if endpoint:
            endpoint = f"{settings.azure_blob_storage_scheme or 'https'}://{endpoint}"
        options = {
            "AZURE_STORAGE_ACCOUNT_NAME": settings.azure_account_name,
            "AZURE_STORAGE_ACCOUNT_KEY": settings.azure_account_key,
            "AZURE_STORAGE_SAS_KEY": settings.azure_sas_token,
            "AZURE_STORAGE_CLIENT_ID": settings.azure_client_id,
            "AZURE_STORAGE_CLIENT_SECRET": settings.azure_client_secret,
            "AZURE_STORAGE_TENANT_ID": settings.azure_tenant_id,
            "AZURE_STORAGE_ENDPOINT": endpoint,
            "AZURE_ALLOW_HTTP": "true" if endpoint and endpoint.startswith("http://") else None,
        }
    else:
        raise StorageError(f"The Delta backend does not support this storage URI scheme: {scheme}")
    return {key: value for key, value in options.items() if value is not None}
