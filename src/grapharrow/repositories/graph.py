"""Parquet-backed repository for node and edge records."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pyarrow as pa  # type: ignore[import-untyped]

from grapharrow.adapters import FileSystemAdapter, ParquetAdapter, PyArrowTabularAdapter
from grapharrow.core import DatabaseLayout, Location
from grapharrow.domain import DatasetFile, SnapshotId, SnapshotManifest
from grapharrow.domain.scan import ScanPredicate
from grapharrow.domain.snapshot import DatasetKind

NODE_SCHEMA = pa.schema(
    [
        ("node_id", pa.int64()),
        ("labels", pa.list_(pa.string())),
        ("properties_json", pa.string()),
    ]
)

EDGE_SCHEMA = pa.schema(
    [
        ("edge_id", pa.int64()),
        ("rel_type", pa.string()),
        ("src_id", pa.int64()),
        ("dst_id", pa.int64()),
        ("properties_json", pa.string()),
    ]
)

NODE_LABEL_SCHEMA = pa.schema([("node_id", pa.int64())])

ADJACENCY_BY_SOURCE_SCHEMA = pa.schema(
    [
        ("src_id", pa.int64()),
        ("dst_id", pa.int64()),
        ("edge_id", pa.int64()),
    ]
)


@dataclass(frozen=True, slots=True)
class NodeRecord:
    """Minimal canonical node representation used by the bootstrap engine."""

    node_id: int
    labels: tuple[str, ...]
    properties: dict[str, object]


@dataclass(frozen=True, slots=True)
class EdgeRecord:
    """Minimal canonical edge representation used by the bootstrap engine."""

    edge_id: int
    rel_type: str
    src_id: int
    dst_id: int
    properties: dict[str, object]


@dataclass(frozen=True, slots=True)
class PersistedGraph:
    """Files generated for a published graph snapshot."""

    staged_dir: str
    nodes: tuple[DatasetFile, ...]
    edges: tuple[DatasetFile, ...]
    node_labels: tuple[DatasetFile, ...]
    node_properties: tuple[DatasetFile, ...]
    adjacency_by_source: tuple[DatasetFile, ...]
    adjacency_by_dest: tuple[DatasetFile, ...]
    edge_properties: tuple[DatasetFile, ...]


class GraphRepository:
    """Persist and load graph records from Parquet files."""

    def __init__(
        self,
        layout: DatabaseLayout,
        parquet_adapter: ParquetAdapter | None = None,
        tabular_adapter: PyArrowTabularAdapter | None = None,
        filesystem: FileSystemAdapter | None = None,
    ) -> None:
        """Bind the layout and adapters used to read and publish canonical and derived datasets."""
        self._layout = layout
        self._filesystem = filesystem or FileSystemAdapter()
        self._parquet = parquet_adapter or ParquetAdapter()
        self._tabular = tabular_adapter or PyArrowTabularAdapter()

    def load_node_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        """Load node tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=NODE_SCHEMA,
        )

    def load_edge_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        """Load edge tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=EDGE_SCHEMA,
        )

    def load_node_label_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        """Load node label index tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=NODE_LABEL_SCHEMA,
        )

    def load_adjacency_by_source_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        """Load adjacency-by-source tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=ADJACENCY_BY_SOURCE_SCHEMA,
        )

    def load_adjacency_by_dest_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        """Load adjacency-by-destination tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=ADJACENCY_BY_SOURCE_SCHEMA,
        )

    def load_node_property_table(
        self,
        files: tuple[DatasetFile, ...],
        *,
        label: str,
        columns: list[str] | None = None,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table:
        """Load the property-projection dataset for ``label``."""

        selected_files = tuple(
            dataset_file
            for dataset_file in files
            if _label_from_node_property_file(dataset_file) == label
        )
        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in selected_files],
            columns=columns,
            predicates=predicates,
        )

    def load_edge_property_table(
        self,
        files: tuple[DatasetFile, ...],
        *,
        rel_type: str,
        columns: list[str] | None = None,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table:
        """Load the property-projection dataset for ``rel_type``."""

        selected_files = tuple(
            dataset_file
            for dataset_file in files
            if _rel_type_from_edge_property_file(dataset_file) == rel_type
        )
        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in selected_files],
            columns=columns,
            predicates=predicates,
        )

    def load_nodes(self, files: tuple[DatasetFile, ...]) -> list[NodeRecord]:
        """Load node records referenced by ``files``."""

        return self.node_records_from_table(self.load_node_table(files))

    def load_edges(self, files: tuple[DatasetFile, ...]) -> list[EdgeRecord]:
        """Load edge records referenced by ``files``."""

        return self.edge_records_from_table(self.load_edge_table(files))

    def node_records_from_table(self, table: pa.Table) -> list[NodeRecord]:
        """Convert a node Arrow table into canonical ``NodeRecord`` objects."""

        node_ids = table["node_id"].to_pylist()
        labels_values = table["labels"].to_pylist()
        properties_values = table["properties_json"].to_pylist()
        records: list[NodeRecord] = []
        for node_id, labels, properties_json in zip(
            node_ids,
            labels_values,
            properties_values,
            strict=True,
        ):
            records.append(
                NodeRecord(
                    node_id=int(node_id),
                    labels=tuple(labels or []),
                    properties=json.loads(properties_json),
                )
            )
        return records

    def edge_records_from_table(self, table: pa.Table) -> list[EdgeRecord]:
        """Convert an edge Arrow table into canonical ``EdgeRecord`` objects."""

        edge_ids = table["edge_id"].to_pylist()
        rel_types = table["rel_type"].to_pylist()
        src_ids = table["src_id"].to_pylist()
        dst_ids = table["dst_id"].to_pylist()
        properties_values = table["properties_json"].to_pylist()
        records: list[EdgeRecord] = []
        for edge_id, rel_type, src_id, dst_id, properties_json in zip(
            edge_ids,
            rel_types,
            src_ids,
            dst_ids,
            properties_values,
            strict=True,
        ):
            records.append(
                EdgeRecord(
                    edge_id=int(edge_id),
                    rel_type=str(rel_type),
                    src_id=int(src_id),
                    dst_id=int(dst_id),
                    properties=json.loads(properties_json),
                )
            )
        return records

    def persist_snapshot(
        self,
        *,
        snapshot_id: SnapshotId,
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
        previous_snapshot: SnapshotManifest | None = None,
        previous_nodes: tuple[NodeRecord, ...] | None = None,
        previous_edges: tuple[EdgeRecord, ...] | None = None,
    ) -> PersistedGraph:
        """Write graph records for ``snapshot_id`` and return manifest file metadata."""

        self._layout.ensure_base_directories()
        staged_dir = self._join(self._layout.staged_dir, f"tx-{snapshot_id.value:06d}")
        if self._filesystem.exists(staged_dir):
            self._filesystem.delete_dir(staged_dir)
        self._filesystem.create_dir(staged_dir)

        staged_nodes_path = self._join(
            staged_dir,
            "data",
            "nodes",
            f"snapshot-{snapshot_id.value:06d}.parquet",
        )
        staged_edges_path = self._join(
            staged_dir,
            "data",
            "edges",
            f"snapshot-{snapshot_id.value:06d}.parquet",
        )
        final_nodes_path = self._join(
            self._layout.nodes_dir,
            f"snapshot-{snapshot_id.value:06d}.parquet",
        )
        final_edges_path = self._join(
            self._layout.edges_dir,
            f"snapshot-{snapshot_id.value:06d}.parquet",
        )
        self._filesystem.create_dir(self._parent(staged_nodes_path))
        self._filesystem.create_dir(self._parent(staged_edges_path))

        same_nodes = (
            previous_snapshot is not None
            and previous_nodes is not None
            and list(previous_nodes) == nodes
        )
        same_edges = (
            previous_snapshot is not None
            and previous_edges is not None
            and list(previous_edges) == edges
        )
        if not same_nodes:
            self._parquet.write_table(_nodes_table(nodes), staged_nodes_path)
            self._filesystem.copy_file(staged_nodes_path, final_nodes_path)
        if not same_edges:
            self._parquet.write_table(_edges_table(edges), staged_edges_path)
            self._filesystem.copy_file(staged_edges_path, final_edges_path)
        node_groups = _unchanged_node_groups(previous_nodes, nodes)
        edge_groups = _unchanged_edge_groups(previous_edges, edges)
        label_groups = _unchanged_label_groups(previous_nodes, nodes)
        topology_groups = _unchanged_topology_groups(previous_edges, edges)

        node_label_files = self._persist_node_label_indexes(
            snapshot_id=snapshot_id,
            nodes=nodes,
            staged_dir=staged_dir,
            reuse_files=previous_snapshot.node_labels if previous_snapshot is not None else (),
            unchanged=label_groups,
        )
        node_property_files = self._persist_node_property_tables(
            snapshot_id=snapshot_id,
            nodes=nodes,
            staged_dir=staged_dir,
            reuse_files=previous_snapshot.node_properties if previous_snapshot is not None else (),
            unchanged=node_groups,
        )
        adjacency_files = self._persist_adjacency_by_source(
            snapshot_id=snapshot_id,
            edges=edges,
            staged_dir=staged_dir,
            reuse_files=previous_snapshot.adjacency_by_source
            if previous_snapshot is not None
            else (),
            unchanged=topology_groups,
        )
        adjacency_by_dest_files = self._persist_adjacency_by_dest(
            snapshot_id=snapshot_id,
            edges=edges,
            staged_dir=staged_dir,
            reuse_files=previous_snapshot.adjacency_by_dest
            if previous_snapshot is not None
            else (),
            unchanged=topology_groups,
        )
        edge_property_files = self._persist_edge_property_tables(
            snapshot_id=snapshot_id,
            edges=edges,
            staged_dir=staged_dir,
            reuse_files=previous_snapshot.edge_properties if previous_snapshot is not None else (),
            unchanged=edge_groups,
        )

        persisted = PersistedGraph(
            staged_dir=self._layout.relative_path(staged_dir),
            nodes=previous_snapshot.nodes
            if same_nodes and previous_snapshot is not None
            else (
                _dataset_file_for(
                    path=final_nodes_path,
                    layout=self._layout,
                    filesystem=self._filesystem,
                    kind="nodes",
                    row_count=len(nodes),
                    min_key=min((record.node_id for record in nodes), default=None),
                    max_key=max((record.node_id for record in nodes), default=None),
                ),
            ),
            edges=previous_snapshot.edges
            if same_edges and previous_snapshot is not None
            else (
                _dataset_file_for(
                    path=final_edges_path,
                    layout=self._layout,
                    filesystem=self._filesystem,
                    kind="edges",
                    row_count=len(edges),
                    min_key=min((record.edge_id for record in edges), default=None),
                    max_key=max((record.edge_id for record in edges), default=None),
                ),
            ),
            node_labels=node_label_files,
            node_properties=node_property_files,
            adjacency_by_source=adjacency_files,
            adjacency_by_dest=adjacency_by_dest_files,
            edge_properties=edge_property_files,
        )
        return persisted

    def cleanup_staged(self, staged_relative_path: str) -> None:
        """Remove a staged transaction directory best-effort."""

        absolute = self._absolute_location(staged_relative_path)
        if self._filesystem.exists(absolute):
            self._filesystem.delete_dir(absolute)

    def _absolute_location(self, relative_path: str) -> str | Path:
        """Resolve a manifest-relative dataset path against the configured database root."""
        if isinstance(self._layout.root, Path):
            return self._layout.root / relative_path
        return f"{str(self._layout.root).rstrip('/')}/{relative_path}"

    def _join(self, base: str | Path, *parts: str) -> str | Path:
        """Join path fragments while supporting both local paths and object-store style strings."""
        if isinstance(base, Path):
            path = base
            for part in parts:
                path = path / part
            return path
        suffix = "/".join(part.strip("/") for part in parts)
        return f"{str(base).rstrip('/')}/{suffix}" if suffix else base

    def _parent(self, location: str | Path) -> str | Path:
        """Return the parent directory for a local path or object-store style location."""
        if isinstance(location, Path):
            return location.parent
        return str(location).rsplit("/", 1)[0]

    def _persist_node_label_indexes(
        self,
        *,
        snapshot_id: SnapshotId,
        nodes: list[NodeRecord],
        staged_dir: Location,
        reuse_files: tuple[DatasetFile, ...] = (),
        unchanged: set[str] | None = None,
    ) -> tuple[DatasetFile, ...]:
        """Write one derived node-label dataset per label to accelerate label-based scans."""
        dataset_files: list[DatasetFile] = []
        by_label: dict[str, list[int]] = {}
        for node in nodes:
            for label in node.labels:
                by_label.setdefault(label, []).append(node.node_id)

        for label, node_ids in sorted(by_label.items()):
            reused = tuple(item for item in reuse_files if Path(item.path).parent.name == label)
            if unchanged is not None and label in unchanged and reused:
                dataset_files.extend(reused)
                continue
            final_path = self._join(
                self._layout.node_labels_dir,
                label,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            staged_path = self._join(
                staged_dir,
                "derived",
                "node_labels",
                label,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            self._filesystem.create_dir(self._parent(staged_path))
            self._filesystem.create_dir(self._parent(final_path))
            node_ids = sorted(node_ids)
            table = pa.Table.from_pydict({"node_id": node_ids}, schema=NODE_LABEL_SCHEMA)
            self._parquet.write_table(table, staged_path)
            self._filesystem.copy_file(staged_path, final_path)
            dataset_files.append(
                _dataset_file_for(
                    path=final_path,
                    layout=self._layout,
                    filesystem=self._filesystem,
                    kind="node_labels",
                    row_count=len(node_ids),
                    min_key=min(node_ids, default=None),
                    max_key=max(node_ids, default=None),
                )
            )
        return tuple(dataset_files)

    def _persist_adjacency_by_source(
        self,
        *,
        snapshot_id: SnapshotId,
        edges: list[EdgeRecord],
        staged_dir: Location,
        reuse_files: tuple[DatasetFile, ...] = (),
        unchanged: set[str] | None = None,
    ) -> tuple[DatasetFile, ...]:
        """Write one adjacency-by-source dataset per relationship type for typed traversals."""
        dataset_files: list[DatasetFile] = []
        by_type: dict[str, list[EdgeRecord]] = {}
        for edge in edges:
            by_type.setdefault(edge.rel_type, []).append(edge)

        for rel_type, typed_edges in sorted(by_type.items()):
            reused = tuple(item for item in reuse_files if Path(item.path).parent.name == rel_type)
            if unchanged is not None and rel_type in unchanged and reused:
                dataset_files.extend(reused)
                continue
            final_path = self._join(
                self._layout.adjacency_by_source_dir,
                rel_type,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            staged_path = self._join(
                staged_dir,
                "derived",
                "adjacency",
                "by_source",
                rel_type,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            self._filesystem.create_dir(self._parent(staged_path))
            self._filesystem.create_dir(self._parent(final_path))
            sorted_edges = sorted(
                typed_edges,
                key=lambda item: (item.src_id, item.dst_id, item.edge_id),
            )
            table = pa.Table.from_pylist(
                [
                    {"src_id": edge.src_id, "dst_id": edge.dst_id, "edge_id": edge.edge_id}
                    for edge in sorted_edges
                ],
                schema=ADJACENCY_BY_SOURCE_SCHEMA,
            )
            self._parquet.write_table(table, staged_path)
            self._filesystem.copy_file(staged_path, final_path)
            dataset_files.append(
                _dataset_file_for(
                    path=final_path,
                    layout=self._layout,
                    filesystem=self._filesystem,
                    kind="adjacency_by_source",
                    row_count=len(sorted_edges),
                    min_key=min((edge.src_id for edge in sorted_edges), default=None),
                    max_key=max((edge.src_id for edge in sorted_edges), default=None),
                )
            )
        return tuple(dataset_files)

    def _persist_adjacency_by_dest(
        self,
        *,
        snapshot_id: SnapshotId,
        edges: list[EdgeRecord],
        staged_dir: Location,
        reuse_files: tuple[DatasetFile, ...] = (),
        unchanged: set[str] | None = None,
    ) -> tuple[DatasetFile, ...]:
        """Write one adjacency-by-destination dataset per relationship type."""
        dataset_files: list[DatasetFile] = []
        by_type: dict[str, list[EdgeRecord]] = {}
        for edge in edges:
            by_type.setdefault(edge.rel_type, []).append(edge)

        for rel_type, typed_edges in sorted(by_type.items()):
            reused = tuple(item for item in reuse_files if Path(item.path).parent.name == rel_type)
            if unchanged is not None and rel_type in unchanged and reused:
                dataset_files.extend(reused)
                continue
            final_path = self._join(
                self._layout.adjacency_by_dest_dir,
                rel_type,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            staged_path = self._join(
                staged_dir,
                "derived",
                "adjacency",
                "by_dest",
                rel_type,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            self._filesystem.create_dir(self._parent(staged_path))
            self._filesystem.create_dir(self._parent(final_path))
            sorted_edges = sorted(
                typed_edges,
                key=lambda item: (item.dst_id, item.src_id, item.edge_id),
            )
            table = pa.Table.from_pylist(
                [
                    {"src_id": edge.src_id, "dst_id": edge.dst_id, "edge_id": edge.edge_id}
                    for edge in sorted_edges
                ],
                schema=ADJACENCY_BY_SOURCE_SCHEMA,
            )
            self._parquet.write_table(table, staged_path)
            self._filesystem.copy_file(staged_path, final_path)
            dataset_files.append(
                _dataset_file_for(
                    path=final_path,
                    layout=self._layout,
                    filesystem=self._filesystem,
                    kind="adjacency_by_dest",
                    row_count=len(sorted_edges),
                    min_key=min((edge.dst_id for edge in sorted_edges), default=None),
                    max_key=max((edge.dst_id for edge in sorted_edges), default=None),
                )
            )
        return tuple(dataset_files)

    def _persist_node_property_tables(
        self,
        *,
        snapshot_id: SnapshotId,
        nodes: list[NodeRecord],
        staged_dir: Location,
        reuse_files: tuple[DatasetFile, ...] = (),
        unchanged: set[str] | None = None,
    ) -> tuple[DatasetFile, ...]:
        """Write one property-columnar dataset per label to avoid JSON decoding on hot reads."""
        dataset_files: list[DatasetFile] = []
        by_label: dict[str, list[NodeRecord]] = {}
        for node in nodes:
            for label in node.labels:
                by_label.setdefault(label, []).append(node)

        for label, typed_nodes in sorted(by_label.items()):
            reused = tuple(item for item in reuse_files if Path(item.path).parent.name == label)
            if unchanged is not None and label in unchanged and reused:
                dataset_files.extend(reused)
                continue
            final_path = self._join(
                self._layout.node_properties_dir,
                label,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            staged_path = self._join(
                staged_dir,
                "derived",
                "node_properties",
                label,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            self._filesystem.create_dir(self._parent(staged_path))
            self._filesystem.create_dir(self._parent(final_path))
            table = _node_property_table(sorted(typed_nodes, key=lambda item: item.node_id))
            self._parquet.write_table(table, staged_path)
            self._filesystem.copy_file(staged_path, final_path)
            node_ids = [node.node_id for node in typed_nodes]
            dataset_files.append(
                _dataset_file_for(
                    path=final_path,
                    layout=self._layout,
                    filesystem=self._filesystem,
                    kind="node_properties",
                    row_count=table.num_rows,
                    min_key=min(node_ids, default=None),
                    max_key=max(node_ids, default=None),
                )
            )
        return tuple(dataset_files)

    def _persist_edge_property_tables(
        self,
        *,
        snapshot_id: SnapshotId,
        edges: list[EdgeRecord],
        staged_dir: Location,
        reuse_files: tuple[DatasetFile, ...] = (),
        unchanged: set[str] | None = None,
    ) -> tuple[DatasetFile, ...]:
        """Write one property-columnar dataset per relationship type for relation-heavy queries."""
        dataset_files: list[DatasetFile] = []
        by_type: dict[str, list[EdgeRecord]] = {}
        for edge in edges:
            by_type.setdefault(edge.rel_type, []).append(edge)

        for rel_type, typed_edges in sorted(by_type.items()):
            reused = tuple(item for item in reuse_files if Path(item.path).parent.name == rel_type)
            if unchanged is not None and rel_type in unchanged and reused:
                dataset_files.extend(reused)
                continue
            final_path = self._join(
                self._layout.edge_properties_dir,
                rel_type,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            staged_path = self._join(
                staged_dir,
                "derived",
                "edge_properties",
                rel_type,
                f"snapshot-{snapshot_id.value:06d}.parquet",
            )
            self._filesystem.create_dir(self._parent(staged_path))
            self._filesystem.create_dir(self._parent(final_path))
            sorted_edges = sorted(typed_edges, key=lambda item: item.edge_id)
            table = _edge_property_table(sorted_edges)
            self._parquet.write_table(table, staged_path)
            self._filesystem.copy_file(staged_path, final_path)
            edge_ids = [edge.edge_id for edge in typed_edges]
            dataset_files.append(
                _dataset_file_for(
                    path=final_path,
                    layout=self._layout,
                    filesystem=self._filesystem,
                    kind="edge_properties",
                    row_count=table.num_rows,
                    min_key=min(edge_ids, default=None),
                    max_key=max(edge_ids, default=None),
                )
            )
        return tuple(dataset_files)


def _unchanged_node_groups(
    previous: tuple[NodeRecord, ...] | None, current: list[NodeRecord]
) -> set[str]:
    before: dict[str, list[NodeRecord]] = {}
    after: dict[str, list[NodeRecord]] = {}
    for node in previous or ():
        for label in node.labels:
            before.setdefault(label, []).append(node)
    for node in current:
        for label in node.labels:
            after.setdefault(label, []).append(node)
    return {label for label, records in after.items() if before.get(label) == records}


def _unchanged_label_groups(
    previous: tuple[NodeRecord, ...] | None, current: list[NodeRecord]
) -> set[str]:
    before: dict[str, list[int]] = {}
    after: dict[str, list[int]] = {}
    for node in previous or ():
        for label in node.labels:
            before.setdefault(label, []).append(node.node_id)
    for node in current:
        for label in node.labels:
            after.setdefault(label, []).append(node.node_id)
    return {label for label, ids in after.items() if before.get(label) == ids}


def _unchanged_edge_groups(
    previous: tuple[EdgeRecord, ...] | None, current: list[EdgeRecord]
) -> set[str]:
    before: dict[str, list[EdgeRecord]] = {}
    after: dict[str, list[EdgeRecord]] = {}
    for edge in previous or ():
        before.setdefault(edge.rel_type, []).append(edge)
    for edge in current:
        after.setdefault(edge.rel_type, []).append(edge)
    return {kind for kind, records in after.items() if before.get(kind) == records}


def _unchanged_topology_groups(
    previous: tuple[EdgeRecord, ...] | None, current: list[EdgeRecord]
) -> set[str]:
    before: dict[str, list[tuple[int, int, int]]] = {}
    after: dict[str, list[tuple[int, int, int]]] = {}
    for edge in previous or ():
        before.setdefault(edge.rel_type, []).append((edge.edge_id, edge.src_id, edge.dst_id))
    for edge in current:
        after.setdefault(edge.rel_type, []).append((edge.edge_id, edge.src_id, edge.dst_id))
    return {kind for kind, records in after.items() if before.get(kind) == records}


def _nodes_table(records: list[NodeRecord]) -> pa.Table:
    """Build the canonical node table written to the snapshot's primary Parquet dataset."""
    return pa.table(
        {
            "node_id": pa.array([record.node_id for record in records], type=pa.int64()),
            "labels": pa.array(
                [list(record.labels) for record in records],
                type=pa.list_(pa.string()),
            ),
            "properties_json": pa.array(
                [json.dumps(record.properties, sort_keys=True) for record in records],
                type=pa.string(),
            ),
        },
        schema=NODE_SCHEMA,
    )


def _edges_table(records: list[EdgeRecord]) -> pa.Table:
    """Build the canonical edge table written to the snapshot's primary Parquet dataset."""
    return pa.table(
        {
            "edge_id": pa.array([record.edge_id for record in records], type=pa.int64()),
            "rel_type": pa.array([record.rel_type for record in records], type=pa.string()),
            "src_id": pa.array([record.src_id for record in records], type=pa.int64()),
            "dst_id": pa.array([record.dst_id for record in records], type=pa.int64()),
            "properties_json": pa.array(
                [json.dumps(record.properties, sort_keys=True) for record in records],
                type=pa.string(),
            ),
        },
        schema=EDGE_SCHEMA,
    )


def _node_property_table(records: list[NodeRecord]) -> pa.Table:
    """Build the per-label property-columnar table used by the Arrow-first read path."""
    property_names = sorted({key for record in records for key in record.properties})
    columns: dict[str, pa.Array] = {
        "node_id": pa.array([record.node_id for record in records], type=pa.int64()),
    }
    for property_name in property_names:
        columns[property_name] = _property_column_array(
            [record.properties.get(property_name) for record in records]
        )
    fields = []
    for name in columns:
        if name == "node_id":
            fields.append(
                pa.field(
                    name,
                    columns[name].type,
                    metadata={"grapharrow.encoding": "reserved-collision"}
                    if name in property_names
                    else None,
                )
            )
            continue
        values = [record.properties.get(name) for record in records]
        types = {type(value) for value in values if value is not None}
        encoding = "native" if len(types) <= 1 and types <= {str, bool, int, float} else "json"
        if types and types <= {int, float} and len(types) > 1:
            encoding = "lossy"
        fields.append(
            pa.field(name, columns[name].type, metadata={"grapharrow.encoding": encoding})
        )
    # Older projections without encoding metadata remain readable through the canonical fallback.
    return pa.Table.from_arrays(list(columns.values()), schema=pa.schema(fields))


def _edge_property_table(records: list[EdgeRecord]) -> pa.Table:
    """Build the per-relationship-type property-columnar table used by the read engine."""
    property_names = sorted({key for record in records for key in record.properties})
    columns: dict[str, pa.Array] = {
        "edge_id": pa.array([record.edge_id for record in records], type=pa.int64()),
        "src_id": pa.array([record.src_id for record in records], type=pa.int64()),
        "dst_id": pa.array([record.dst_id for record in records], type=pa.int64()),
    }
    for property_name in property_names:
        columns[property_name] = _property_column_array(
            [record.properties.get(property_name) for record in records]
        )
    return pa.table(columns)


def _property_column_array(values: list[object]) -> pa.Array:
    """Choose a stable Arrow column type for one observed property across many records."""
    non_null = [value for value in values if value is not None]
    if not non_null:
        return pa.array(values, type=pa.null())
    if all(isinstance(value, bool) for value in non_null):
        return pa.array(values, type=pa.bool_())
    if all(isinstance(value, int) and not isinstance(value, bool) for value in non_null):
        return pa.array(values, type=pa.int64())
    if all(isinstance(value, int | float) and not isinstance(value, bool) for value in non_null):
        return pa.array(
            [float(cast(int | float, value)) if value is not None else None for value in values],
            type=pa.float64(),
        )
    if all(isinstance(value, str) for value in non_null):
        return pa.array(values, type=pa.string())
    return pa.array(
        [json.dumps(value, sort_keys=True) if value is not None else None for value in values],
        type=pa.string(),
    )


def _label_from_node_property_file(dataset_file: DatasetFile) -> str | None:
    """Recover the label name encoded in a derived node-properties dataset path."""
    path = Path(dataset_file.path)
    if len(path.parts) < 3 or path.parts[0] != "derived" or path.parts[1] != "node_properties":
        return None
    return path.parts[2]


def _rel_type_from_edge_property_file(dataset_file: DatasetFile) -> str | None:
    """Recover the relationship type encoded in a derived edge-properties dataset path."""
    path = Path(dataset_file.path)
    if len(path.parts) < 3 or path.parts[0] != "derived" or path.parts[1] != "edge_properties":
        return None
    return path.parts[2]


def _dataset_file_for(
    *,
    path: str | Path,
    layout: DatabaseLayout,
    filesystem: FileSystemAdapter,
    kind: DatasetKind,
    row_count: int,
    min_key: int | None,
    max_key: int | None,
) -> DatasetFile:
    """Build the manifest entry for one freshly written dataset file, including checksum data."""
    return DatasetFile(
        path=layout.relative_path(path),
        row_count=row_count,
        kind=kind,
        checksum=_compute_sha256(path, filesystem=filesystem),
        checksum_algorithm="sha256",
        min_key=min_key,
        max_key=max_key,
    )


def _compute_sha256(path: str | Path, *, filesystem: FileSystemAdapter) -> str:
    """Compute the SHA-256 checksum stored in snapshot manifests for integrity verification."""
    hasher = hashlib.sha256()
    resolved_filesystem, normalized_path = filesystem.resolve(path)
    with resolved_filesystem.open_input_file(normalized_path) as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


__all__ = ["EdgeRecord", "GraphRepository", "NodeRecord", "PersistedGraph"]
