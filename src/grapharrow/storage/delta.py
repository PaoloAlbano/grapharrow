"""An optional Delta Lake backend with one atomic transaction per graph commit."""

from __future__ import annotations

from collections import Counter
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock, RLock
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from uuid import uuid4

import orjson
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.dataset as ds  # type: ignore[import-untyped]
import pyarrow.fs as pafs  # type: ignore[import-untyped]
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError, DeltaError, TableNotFoundError
from deltalake.fs import DeltaStorageHandler
from deltalake.transaction import CommitProperties

from grapharrow.adapters.snapshot_tables import SnapshotTableCache
from grapharrow.adapters.writer import WriterLock, writer_locked
from grapharrow.config import GraphArrowSettings
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import (
    CatalogError,
    CommitOutcomeUnknownError,
    StorageError,
    WriteConflictError,
)
from grapharrow.domain import (
    CatalogVersion,
    CurrentSnapshotPointer,
    SnapshotId,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.domain.records import EdgeRecord, NodeRecord
from grapharrow.domain.snapshot import IdempotencyReceipt
from grapharrow.repositories.catalog import deserialize_catalog, serialize_catalog
from grapharrow.repositories.graph import EDGE_SCHEMA, NODE_SCHEMA, _edges_table, _nodes_table
from grapharrow.storage.contracts import CatalogStore, GraphStore, SnapshotStore, StorageMaintenance
from grapharrow.storage.delta_graph import DeltaGraphStore, dataset_ref
from grapharrow.storage.models import (
    InitializationResult,
    RecoveryReport,
    RetentionPlan,
    VerificationReport,
)

if TYPE_CHECKING:
    from grapharrow.use_cases.graph import GraphState

_FORMAT = "grapharrow-delta-v1"
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


@dataclass(frozen=True, slots=True)
class _ReadVersion:
    table: DeltaTable
    manifest: SnapshotManifest
    catalog: VersionedCatalog


class DeltaStorageBackend:
    """Use a partitioned Delta table as the sole source of truth for one database."""

    def __init__(
        self,
        *,
        layout: DatabaseLayout,
        settings: GraphArrowSettings,
        snapshot_tables: SnapshotTableCache | None = None,
    ) -> None:
        self._layout = layout
        self._uri = str(layout.delta_dir)
        self._options = _storage_options(layout.root, settings)
        self._writer = _DeltaWriter()
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
            return DeltaTable(self._uri, version=version, storage_options=self._options)
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
        return _partition(self._read_table(snapshot_id), kind, columns, expression)

    def _read_table(self, snapshot_id: SnapshotId) -> DeltaTable:
        cached = self._read_version
        if cached is not None and cached.manifest.snapshot_id == snapshot_id:
            return cached.table
        return self._require_table(snapshot_id)

    def _describe_read_version(self, table: DeltaTable) -> _ReadVersion:
        """Retain only the latest observed version, with a handle used exclusively for reads."""
        snapshot_id = SnapshotId(table.version() + 1)
        with self._read_version_lock:
            cached = self._read_version
            if cached is not None and cached.manifest.snapshot_id == snapshot_id:
                return cached
            head, catalog = _metadata(table)
            entry = _ReadVersion(table, head.manifest(snapshot_id, catalog.version), catalog)
            if self._retain_read_version and (
                cached is None or cached.manifest.snapshot_id.value < snapshot_id.value
            ):
                self._read_version = entry
            return entry

    def capture_snapshot(self) -> SnapshotManifest:
        return self.bootstrap_snapshot()

    def bootstrap_snapshot(self) -> SnapshotManifest:
        table = self._try_open()
        if table is not None:
            return self._describe_read_version(table).manifest
        with self._writer.hold():
            table = self._try_open()
            if table is not None:
                return self._describe_read_version(table).manifest
            self._check_native_layout()
            catalog = VersionedCatalog.empty(version=CatalogVersion(1))
            head = _new_head(0, 0, {}, {}, None)
            data = _metadata_table(head, catalog)
            try:
                write_deltalake(
                    self._uri,
                    data,
                    partition_by=["kind"],
                    configuration={
                        "delta.isolationLevel": "Serializable",
                        "delta.enableExpiredLogCleanup": "false",
                    },
                    storage_options=self._options,
                    commit_properties=CommitProperties(max_commit_retries=0),
                )
            except (DeltaError, OSError) as exc:
                table = self._try_open()
                if table is None:
                    raise StorageError("Could not initialize the Delta graph.") from exc
                return self._describe_read_version(table).manifest
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
        for value in range(head.snapshot_id.value, 0, -1):
            manifest = self.load_manifest(SnapshotId(value))
            yield _pointer(manifest).manifest_path, manifest

    def publish(
        self, manifest: SnapshotManifest, *, base: SnapshotManifest | None = None
    ) -> CurrentSnapshotPointer:
        raise StorageError("Publish Delta snapshots through graph transactions.")

    def idempotency_base(
        self, pointer: CurrentSnapshotPointer, key_hash: str
    ) -> tuple[SnapshotManifest, IdempotencyReceipt | None]:
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
        table = self._require_table(current.snapshot_id)
        head, old_catalog = _metadata(table)
        if catalog.version.value <= old_catalog.version.value:
            if catalog == old_catalog:
                return f"delta/{current.snapshot_id.value}/catalog"
            raise WriteConflictError("Catalog changed. Reload it before writing a new version.")
        candidate = _new_head(
            head.node_count, head.edge_count, head.labels, head.relationships, None
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

        base = self._require_table(previous_state.snapshot.snapshot_id)
        partitions: dict[str, pa.Table] = {}
        node_records = tuple(nodes)
        edge_records = tuple(edges)
        node_table = previous_state.node_table
        if node_records != previous_state.nodes:
            node_table = _nodes_table(nodes)
            partitions["nodes"] = node_table
        if edge_records != previous_state.edges:
            edge_table = edge_table if edge_table is not None else _edges_table(edges)
            partitions["edges"] = edge_table
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
        )
        manifest = self._commit(base, head, catalog, partitions)
        return GraphState(catalog, manifest, node_table, edge_table, node_records, edge_records)

    def _commit(
        self,
        base: DeltaTable,
        head: _Head,
        catalog: VersionedCatalog,
        partitions: dict[str, pa.Table],
    ) -> SnapshotManifest:
        snapshot_id = SnapshotId(base.version() + 2)
        data = pa.concat_tables(
            [
                _metadata_table(head, catalog),
                *(_rows(part, kind) for kind, part in partitions.items()),
            ]
        )
        kinds = ", ".join(f"'{kind}'" for kind in ("metadata", *partitions))
        try:
            # The Python writer reloads the table URI when passed a string. Pass the
            # pinned table, and disallow rebasing a graph mutation on a newer version.
            write_deltalake(
                base,
                data,
                mode="overwrite",
                predicate=f"kind IN ({kinds})",
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
        yield None

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
            manifest = _manifest(self._require_table(None))
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
        head, catalog = _metadata(base)
        manifest = head.manifest(SnapshotId(base.version() + 1), catalog.version)
        candidate = _new_head(
            head.node_count, head.edge_count, head.labels, head.relationships, None
        )
        return self._commit(
            base,
            candidate,
            catalog,
            {
                "nodes": self._graph.load_node_table(manifest.nodes),
                "edges": self._graph.load_edge_table(manifest.edges),
            },
        )

    def vacuum(self) -> tuple[str, ...]:
        raise StorageError("Delta vacuum is unavailable while historical snapshots are retained.")

    def plan_retention(self, *, keep_last: int, min_age_hours: int = 0) -> RetentionPlan:
        raise StorageError("Snapshot retention planning is currently available for Native storage.")


def _new_head(
    node_count: int,
    edge_count: int,
    labels: dict[str, int],
    relationships: dict[str, int],
    receipt: IdempotencyReceipt | None,
) -> _Head:
    return _Head(
        uuid4().hex,
        datetime.now(UTC).isoformat(timespec="seconds"),
        node_count,
        edge_count,
        labels,
        relationships,
        receipt,
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


def _metadata_table(head: _Head, catalog: VersionedCatalog) -> pa.Table:
    payload = {"format": _FORMAT, "graph": asdict(head), "catalog": serialize_catalog(catalog)}
    return _rows(pa.table({"properties_json": [orjson.dumps(payload).decode()]}), "metadata")


def _partition(
    table: DeltaTable, kind: str, columns: list[str], expression: object = None
) -> pa.Table:
    try:
        dataset = table.to_pyarrow_dataset(file_pruning_predicate=[("kind", "=", kind)])
        return dataset.to_table(columns=columns, filter=expression)
    except (DeltaError, OSError, pa.ArrowException) as exc:
        raise StorageError(f"Could not read the Delta {kind} partition.") from exc


def _metadata(table: DeltaTable) -> tuple[_Head, VersionedCatalog]:
    rows = _partition(table, "metadata", ["properties_json"]).column(0)
    if len(rows) != 1:
        raise StorageError("Expected exactly one metadata row at the captured Delta version.")
    try:
        payload = orjson.loads(rows[0].as_py())
        if payload["format"] != _FORMAT:
            raise ValueError("Unrecognized graph layout")
        data = payload["graph"]
        receipt = data["receipt"]
        if receipt is not None:
            data["receipt"] = IdempotencyReceipt(**receipt)
        return _Head(**data), deserialize_catalog(payload["catalog"])
    except (CatalogError, KeyError, TypeError, ValueError) as exc:
        raise StorageError("Invalid GraphArrow metadata in the Delta log.") from exc


def _manifest(table: DeltaTable) -> SnapshotManifest:
    head, catalog = _metadata(table)
    return head.manifest(SnapshotId(table.version() + 1), catalog.version)


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
