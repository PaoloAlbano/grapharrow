# mypy: disable-error-code=no-untyped-call
"""Embedded Python API exposed by the GraphArrow package."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from grapharrow.adapters import FileSystemAdapter
from grapharrow.app import GraphArrowApplication, build_application
from grapharrow.core import DatabaseLayout
from grapharrow.core.errors import TransactionError
from grapharrow.domain import (
    CatalogVersion,
    CurrentSnapshotPointer,
    IndexDefinition,
    IndexKind,
    PropertyOwnerKind,
    SnapshotId,
    SnapshotManifest,
    VersionedCatalog,
)
from grapharrow.interfaces.cypher_guard import query_requires_write
from grapharrow.use_cases import (
    InitializationResult,
    QueryResult,
    RecoveryReport,
    VerificationReport,
)

if TYPE_CHECKING:
    from collections.abc import Mapping


SessionMode = Literal["read", "write"]


@dataclass(slots=True)
class Session:
    """A lightweight session bound to an embedded database."""

    database: Database
    mode: SessionMode
    _closed: bool = field(default=False, init=False, repr=False)

    def __enter__(self) -> Session:
        """Allow the embedded session to be used as a context manager by Python callers."""
        self._ensure_open()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Close the session automatically when leaving a ``with`` block."""
        self.close()

    @property
    def closed(self) -> bool:
        """Report whether the session has been closed."""

        return self._closed

    def close(self) -> None:
        """Close this session. Later operations raise TransactionError."""

        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise TransactionError("Session is closed.")

    def query(
        self,
        query: str,
        params: Mapping[str, object] | None = None,
    ) -> QueryResult:
        """Execute one Cypher statement, enforcing read sessions against accidental writes."""

        self._ensure_open()
        if self.mode == "read" and query_requires_write(query):
            raise TransactionError("Write queries require a write transaction.")
        return self.database._app.use_cases.cypher.execute(query, dict(params or {}))

    def add_node(
        self,
        *,
        labels: list[str],
        properties: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        """Create one node through the embedded imperative API and return its Python payload."""

        self._ensure_open()
        if self.mode == "read":
            raise TransactionError("Node creation requires a write transaction.")
        _, node = self.database._app.use_cases.graph.create_node(
            labels=tuple(labels),
            properties=dict(properties or {}),
        )
        return {
            "id": node.node_id,
            "labels": list(node.labels),
            "properties": dict(node.properties),
        }

    def add_nodes_bulk(
        self,
        data: object,
        *,
        default_labels: list[str] | None = None,
    ) -> tuple[dict[str, object], ...]:
        """Create many nodes from supported bulk inputs through the shared bulk-ingest adapter."""

        self._ensure_open()
        if self.mode == "read":
            raise TransactionError("Node creation requires a write transaction.")

        table = self.database._app.adapters.bulk_input.to_table(data)
        _, nodes = self.database._app.use_cases.graph.create_nodes_bulk(
            table=table,
            default_labels=tuple(default_labels or ()),
        )
        return tuple(
            {
                "id": node.node_id,
                "labels": list(node.labels),
                "properties": dict(node.properties),
            }
            for node in nodes
        )

    def add_edge(
        self,
        *,
        src: object,
        rel_type: str,
        dst: object,
        properties: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        """Create one relationship between existing node ids through the embedded imperative API."""

        self._ensure_open()
        if self.mode == "read":
            raise TransactionError("Edge creation requires a write transaction.")
        if not isinstance(src, int) or not isinstance(dst, int):
            raise TypeError(
                "The bootstrap imperative edge API currently requires integer node ids."
            )

        _, edge = self.database._app.use_cases.graph.create_edge(
            src_id=src,
            dst_id=dst,
            rel_type=rel_type,
            properties=dict(properties or {}),
        )
        return {
            "id": edge.edge_id,
            "type": edge.rel_type,
            "source": edge.src_id,
            "target": edge.dst_id,
            "properties": dict(edge.properties),
        }

    def add_edges_bulk(
        self,
        data: object,
        *,
        source_key: tuple[str, str] | None = None,
        target_key: tuple[str, str] | None = None,
    ) -> tuple[dict[str, object], ...]:
        """Create many relationships from supported bulk inputs, optionally using business keys."""

        self._ensure_open()
        if self.mode == "read":
            raise TransactionError("Edge creation requires a write transaction.")

        table = self.database._app.adapters.bulk_input.to_table(data)
        _, edges = self.database._app.use_cases.graph.create_edges_bulk(
            table=table,
            source_key=source_key,
            target_key=target_key,
        )
        return tuple(
            {
                "id": edge.edge_id,
                "type": edge.rel_type,
                "source": edge.src_id,
                "target": edge.dst_id,
                "properties": dict(edge.properties),
            }
            for edge in edges
        )

    def add_triple(
        self,
        subject: object,
        predicate: str,
        object_: object,
        *,
        properties: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        """Expose edge creation with subject-predicate-object naming for higher-level callers."""

        return self.add_edge(
            src=subject,
            rel_type=predicate,
            dst=object_,
            properties=properties,
        )


class Database:
    """Represents a single embedded GraphArrow database."""

    def __init__(
        self,
        path: str | Path,
        *,
        filesystem: FileSystemAdapter | None = None,
    ) -> None:
        """Build the embedded application container for one local or object-store database."""
        self._path = path
        self._app: GraphArrowApplication = build_application(
            self._path,
            filesystem=filesystem,
        )

    @property
    def path(self) -> str | Path:
        """Return the resolved database location."""

        return self._app.layout.root

    @property
    def layout(self) -> DatabaseLayout:
        """Return the resolved database layout."""

        return self._app.layout

    def initialize(self) -> InitializationResult:
        """Bootstrap the database layout and ensure the first empty catalog and snapshot exist."""

        return self._app.use_cases.admin.initialize()

    def load_catalog(self, version: CatalogVersion | None = None) -> VersionedCatalog:
        """Load the current catalog, or a historical version when one is explicitly requested."""

        return self._app.repositories.catalog.load(version=version)

    def write_catalog(self, catalog: VersionedCatalog) -> str | Path:
        """Persist a catalog version directly through the embedded repository surface."""

        return self._app.repositories.catalog.write(catalog)

    def list_indexes(self) -> tuple[IndexDefinition, ...]:
        """Return logical index declarations stored in the current catalog."""

        return self._app.use_cases.indexes.list_indexes()

    def create_index(
        self,
        *,
        name: str,
        kind: IndexKind,
        owner_kind: PropertyOwnerKind,
        owner_name: str,
        property_names: tuple[str, ...],
        if_not_exists: bool = False,
        case_sensitive: bool = True,
    ) -> IndexDefinition:
        """Declare a logical index through the shared index use case."""

        return self._app.use_cases.indexes.create_index(
            name=name,
            kind=kind,
            owner_kind=owner_kind,
            owner_name=owner_name,
            property_names=property_names,
            if_not_exists=if_not_exists,
            case_sensitive=case_sensitive,
        )

    def drop_index(self, *, name: str, if_exists: bool = False) -> IndexDefinition | None:
        """Remove a logical index declaration through the shared index use case."""

        return self._app.use_cases.indexes.drop_index(name=name, if_exists=if_exists)

    def load_current_snapshot_pointer(self) -> CurrentSnapshotPointer:
        """Load the snapshot pointer that readers currently treat as the visible head state."""

        return self._app.repositories.snapshot.load_current_pointer()

    def load_snapshot(self, snapshot_id: SnapshotId | None = None) -> SnapshotManifest:
        """Load the current snapshot manifest or one historical manifest by id."""

        return self._app.repositories.snapshot.load_manifest(snapshot_id=snapshot_id)

    def publish_snapshot(self, manifest: SnapshotManifest) -> CurrentSnapshotPointer:
        """Publish a manifest directly, mainly for low-level embedded maintenance workflows."""

        return self._app.repositories.snapshot.publish(manifest)

    def verify(self, *, verify_checksums: bool = False) -> VerificationReport:
        """Run the admin verification flow from the embedded API."""

        return self._app.use_cases.admin.verify(verify_checksums=verify_checksums)

    def recover(
        self,
        *,
        dry_run: bool = True,
        cleanup_orphan_staged: bool = True,
        verify_checksums: bool = False,
    ) -> RecoveryReport:
        """Run the recovery workflow from the embedded API with the same safety switches."""

        return self._app.use_cases.admin.recover(
            dry_run=dry_run,
            cleanup_orphan_staged=cleanup_orphan_staged,
            verify_checksums=verify_checksums,
        )

    def compact(self) -> SnapshotManifest:
        """Rewrite the current graph state into a fresh snapshot and rebuild derived datasets."""

        return self._app.use_cases.admin.compact()

    def vacuum(self) -> tuple[str, ...]:
        """Remove unreachable manifests, parquet files, and orphan staged directories."""

        return self._app.use_cases.admin.vacuum()

    def session(self) -> Session:
        """Create the default embedded session, which is currently read-only by design."""

        return Session(database=self, mode="read")

    def read_transaction(self) -> Session:
        """Create a read-only embedded session for callers that want explicit intent."""

        return Session(database=self, mode="read")

    def write_transaction(self) -> Session:
        """Create a write-capable embedded session for imperative writes or write Cypher."""

        return Session(database=self, mode="write")
