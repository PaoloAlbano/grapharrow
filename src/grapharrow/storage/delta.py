"""An optional Delta Lake backend with one atomic transaction per graph commit."""

from __future__ import annotations

from collections import Counter
from collections.abc import Generator, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from functools import cached_property
from pathlib import Path
from threading import Lock, RLock
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
    NODE_SCHEMA,
    _edge_ranges,
    _edges_table,
    _nodes_table,
)
from grapharrow.storage.contracts import CatalogStore, GraphStore, SnapshotStore, StorageMaintenance
from grapharrow.storage.delta_graph import DeltaEdgeRange, DeltaGraphStore, dataset_ref
from grapharrow.storage.delta_inventory import inspect_delta_storage, plan_delta_retention
from grapharrow.storage.delta_retention import MANAGED_DESCRIPTION, DeltaRetentionCoordinator
from grapharrow.storage.models import (
    DeltaPinRecoveryResult,
    DeltaRetentionResult,
    DeltaStorageInventory,
    InitializationResult,
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
_DEFAULT_EDGE_RANGE_SIZE = 8192
# The string interval contains exactly the partition names with the 'edges-' prefix.
_EDGE_PARTITION_PREDICATE = "kind = 'edges' OR (kind >= 'edges-' AND kind < 'edges.')"
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


class _DeltaWriter:
    """Serialize operations within one application. Delta arbitrates across replicas."""

    local_root: Path | None = None

    def __init__(self) -> None:
        self._lock = RLock()

    @contextmanager
    def hold(self) -> Iterator[None]:
        with self._lock:
            yield


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
    table: DeltaTable
    manifest: SnapshotManifest
    catalog: VersionedCatalog
    head: _Head

    @cached_property
    def graph_files(self) -> dict[str, tuple[DatasetFile, ...]]:
        try:
            actions = pa.table(self.table.get_add_actions(flatten=True))
            rows = actions.select(["path", "partition.kind", "num_records"]).to_pylist()
        except (DeltaError, pa.ArrowException, OSError) as exc:
            raise StorageError("Could not read the active Delta graph files.") from exc
        files: dict[str, list[DatasetFile]] = {}
        for row in rows:
            kind = row["partition.kind"]
            if kind == "nodes" or kind.startswith("edges-"):
                files.setdefault(kind, []).append(
                    DatasetFile(
                        row["path"],
                        row["num_records"] or 0,
                        "nodes" if kind == "nodes" else "edges",
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
        self._read_version: _ReadVersion | None = None
        self._read_version_lock = Lock()
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
        if self._managed is None:
            self._require_table(snapshot_id)
        return self._retention.pin(snapshot_id.value) if self._managed else nullcontext()

    @cached_property
    def _retention(self) -> DeltaRetentionCoordinator:
        return DeltaRetentionCoordinator(self._layout, self._filesystem, self._replica_id)

    def _read_edge_ranges(self, snapshot_id: SnapshotId) -> tuple[DeltaEdgeRange, ...]:
        return self._describe_read_version(self._read_table(snapshot_id)).edge_ranges

    def _read_node_files(self, snapshot_id: SnapshotId) -> tuple[DatasetFile, ...]:
        version = self._describe_read_version(self._read_table(snapshot_id))
        return version.graph_files.get("nodes", ())

    def _read_table(self, snapshot_id: SnapshotId) -> DeltaTable:
        cached = self._read_version
        if cached is not None and cached.manifest.snapshot_id == snapshot_id:
            return cached.table
        return self._require_table(snapshot_id)

    def _describe_read_version(self, table: DeltaTable) -> _ReadVersion:
        with self.pin_snapshot(SnapshotId(table.version() + 1)):
            return self._describe_pinned_read_version(table)

    def _describe_pinned_read_version(self, table: DeltaTable) -> _ReadVersion:
        """Retain only the latest observed version, with a handle used exclusively for reads."""
        snapshot_id = SnapshotId(table.version() + 1)
        with self._read_version_lock:
            cached = self._read_version
            if cached is not None and cached.manifest.snapshot_id == snapshot_id:
                return cached
            head, catalog = _metadata(table)
            entry = _ReadVersion(table, self._manifest(table, head, catalog), catalog, head)
            if self._retain_read_version and (
                cached is None or cached.manifest.snapshot_id.value < snapshot_id.value
            ):
                self._read_version = entry
            return entry

    @cached_property
    def _log_storage(self) -> DeltaStorageHandler:
        return DeltaStorageHandler(self._uri, self._options)

    def _commit_info(self, version: int) -> dict[str, Any]:
        path = f"_delta_log/{version:020d}.json"
        try:
            # Supplying the size lets the handler skip an additional object-store HEAD.
            info = self._log_storage.get_file_info([path])[0]
            if info.type != pafs.FileType.File:
                raise StorageError(f"Missing Delta log file for version {version}.")
            storage = DeltaStorageHandler(self._uri, self._options, known_sizes={path: info.size})
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
        self, table: DeltaTable, head: _Head, catalog: VersionedCatalog
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

    @contextmanager
    def borrow_current_snapshot(self) -> Iterator[SnapshotManifest]:
        """Hold the selected snapshot from its metadata read through the caller's work."""
        cached = self._read_version
        if cached is not None and self._cached_snapshot_is_current(cached.manifest.snapshot_id):
            with self.pin_snapshot(cached.manifest.snapshot_id):
                yield cached.manifest
            return
        with self._borrow_bootstrapped_snapshot() as snapshot:
            yield snapshot

    def _cached_snapshot_is_current(self, snapshot_id: SnapshotId) -> bool:
        # GraphArrow retains the contiguous commit log, including expired snapshots.
        path = f"_delta_log/{snapshot_id.value:020d}.json"
        try:
            if isinstance(self._layout.delta_dir, Path):
                (self._layout.delta_dir / path).stat()
                return False
            return bool(self._log_storage.get_file_info([path])[0].type == pafs.FileType.NotFound)
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
        source = self._open_or_create()
        if isinstance(source, SnapshotManifest):
            with self.pin_snapshot(source.snapshot_id):
                yield source
            return
        with self.pin_snapshot(SnapshotId(source.version() + 1)):
            yield self._describe_pinned_read_version(source).manifest

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
            rows = _partition(
                table,
                _receipt_partition(key_hash),
                _RECEIPT_COLUMNS,
                ds.field("key_hash") == key_hash,
            ).to_pylist()
            if len(rows) > 1:
                raise StorageError("Duplicate idempotency receipts in the Delta snapshot.")
            receipt = IdempotencyReceipt(**rows[0]) if rows else None
            return manifest, receipt

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
            if node_records != previous_state.nodes:
                node_table = _nodes_table(nodes)
                partitions["nodes"] = node_table
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
                receipts = _partition(base, kind, _RECEIPT_COLUMNS)
                receipt = pa.Table.from_pylist(
                    [asdict(idempotency_receipt)],
                    schema=pa.schema([_SCHEMA.field(name) for name in _RECEIPT_COLUMNS]),
                )
                partitions[kind] = pa.concat_tables([receipts.cast(receipt.schema), receipt])

            head = _new_head(
                len(nodes),
                len(edges),
                dict(Counter(label for node in nodes for label in set(node.labels))),
                dict(Counter(edge.rel_type for edge in edges)),
                idempotency_receipt,
                previous_head.edge_range_size,
                metadata_version=base.version() + 1,
            )
            manifest = self._commit(base, head, catalog, partitions)
            return GraphState(catalog, manifest, node_table, edge_table, node_records, edge_records)

    def _commit(
        self,
        base: DeltaTable,
        head: _Head,
        catalog: VersionedCatalog,
        partitions: dict[str, pa.Table],
        *,
        replace_all_edges: bool = False,
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
        )
        predicate = f"kind IN ({kinds})"
        if replace_all_edges:
            predicate += f" OR ({_EDGE_PARTITION_PREDICATE})"
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
    def edit_edge_properties_by_id(
        self, *, edge_id: int, rel_type: str | None, properties: dict[str, object]
    ) -> Generator[tuple[EdgeRecord | None] | None, None, None]:
        """Replace one canonical range after the caller builds its result successfully."""
        with self._writer.hold(), self.borrow_current_snapshot() as snapshot:
            current = self._describe_pinned_read_version(self._read_table(snapshot.snapshot_id))
            width = current.head.edge_range_size
            if not width or current.catalog.constraints:
                yield None
                return
            kind = _edge_partition((edge_id - 1) // width)
            table = _partition(current.table, kind, EDGE_SCHEMA.names).cast(EDGE_SCHEMA)
            positions = pc.indices_nonzero(pc.equal(table["edge_id"], edge_id))
            if not len(positions):
                yield (None,)
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
                return
            base = self._require_table(snapshot.snapshot_id)
            self._commit_point(base, current.head.commit_id, kind, after)

    def _commit_point(
        self, base: DeltaTable, metadata_id: str, kind: str, edge: EdgeRecord
    ) -> None:
        commit_id = uuid4().hex
        base_version = base.version()
        # The guard matches no row on the pinned base and matches any replacement
        # metadata row during conflict checking. The update only writes the edge file.
        predicate = (
            f"(kind = '{kind}' AND edge_id = {edge.edge_id}) OR "
            f"(kind = 'metadata' AND (key_hash IS NULL OR key_hash <> '{metadata_id}'))"
        )
        try:
            metrics = base.update(
                predicate=predicate,
                new_values={"properties_json": property_codec.dumps(edge.properties)},
                commit_properties=CommitProperties(
                    custom_metadata={_POINT_COMMIT_ID: commit_id},
                    max_commit_retries=_POINT_COMMIT_RETRIES,
                ),
            )
        except (DeltaError, OSError) as exc:
            # A successful rebase can publish at any version after the original base.
            # A retried conditional put can report a conflict with its own commit.
            try:
                latest = self._require_table(None).version()
                for version in range(latest, base_version, -1):
                    if self._commit_info(version).get(_POINT_COMMIT_ID) == commit_id:
                        return
            except StorageError as read_error:
                raise CommitOutcomeUnknownError(
                    "Could not confirm the Delta commit outcome."
                ) from read_error
            if isinstance(exc, CommitFailedError):
                raise WriteConflictError(_CONFLICT) from exc
            raise CommitOutcomeUnknownError("Could not confirm the Delta commit outcome.") from exc
        if metrics.get("num_updated_rows") != 1:
            raise CommitOutcomeUnknownError("Delta did not report exactly one updated edge.")

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
            )
            partitions = _edge_partitions(self._graph.load_edge_table(manifest.edges), width)
            partitions["nodes"] = self._graph.load_node_table(manifest.nodes)
            return self._commit(
                base,
                candidate,
                catalog,
                partitions,
                replace_all_edges=True,
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
    payload = {
        "format": _MANAGED_FORMAT if managed else _FORMAT,
        "graph": asdict(head),
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
    table: DeltaTable, kind: str, columns: list[str], expression: object = None
) -> pa.Table:
    try:
        dataset = table.to_pyarrow_dataset(
            file_pruning_predicate=(
                _EDGE_PARTITION_PREDICATE if kind == "edges" else [("kind", "=", kind)]
            )
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


def _metadata(table: DeltaTable) -> tuple[_Head, VersionedCatalog]:
    rows = _partition(table, "metadata", ["properties_json", "key_hash"])
    if rows.num_rows != 1:
        raise StorageError("Expected exactly one metadata row at the captured Delta version.")
    try:
        payload = orjson.loads(rows["properties_json"][0].as_py())
        data = payload["graph"]
        expected_format = (
            _MANAGED_FORMAT if table.metadata().description == MANAGED_DESCRIPTION else _FORMAT
        )
        if payload["format"] != expected_format or "metadata_version" not in data:
            raise StorageError("Unsupported GraphArrow Delta layout. Use a new database path.")
        width = data["edge_range_size"]
        if type(width) is not int or not 0 <= width < 2**63:
            raise ValueError("Invalid edge range size")
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


def _edge_partition(bucket: int) -> str:
    return f"edges-{bucket:020d}"


def _edge_partitions(table: pa.Table, width: int) -> dict[str, pa.Table]:
    if not width:
        return {"edges": table}
    if not table.num_rows:
        return {}
    return {_edge_partition(bucket): rows for bucket, rows in _edge_ranges(table, width)}


def _changed_edge_partitions(
    table: pa.Table, previous: pa.Table, width: int
) -> dict[str, pa.Table]:
    if not width:
        return {"edges": table}
    old = _edge_partitions(previous, width)
    current = _edge_partitions(table, width)
    changed = {
        kind: rows
        for kind, rows in current.items()
        if kind not in old or not rows.equals(old[kind])
    }
    changed.update({kind: EDGE_SCHEMA.empty_table() for kind in old.keys() - current.keys()})
    return changed


def _can_patch_properties(edge: EdgeRecord, properties: dict[str, object]) -> bool:
    return all(
        type(value) in (str, bool, int, float)
        and type(edge.properties.get(name)) is type(value)
        and (type(value) is not int or -(2**63) <= value < 2**63)
        for name, value in properties.items()
    )


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
