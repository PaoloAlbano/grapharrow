"""Parquet-backed repository for node and edge records."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Generator, Iterable
from dataclasses import replace
from pathlib import Path, PurePosixPath
from typing import cast
from uuid import uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from grapharrow.adapters import FileSystemAdapter, ParquetAdapter, PyArrowTabularAdapter
from grapharrow.adapters.checksum import sha256_file as _compute_sha256
from grapharrow.core import DatabaseLayout, property_codec
from grapharrow.domain import DatasetFile, SnapshotId, SnapshotManifest
from grapharrow.domain.records import EdgeRecord as EdgeRecord
from grapharrow.domain.records import NodeRecord as NodeRecord
from grapharrow.domain.scan import ScanPredicate
from grapharrow.domain.snapshot import DatasetKind
from grapharrow.storage.models import EdgePropertyPatch, NodePropertyPatch
from grapharrow.storage.models import PersistedGraph as PersistedGraph

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
EDGE_TOPOLOGY_SCHEMA = EDGE_SCHEMA.remove(EDGE_SCHEMA.get_field_index("properties_json"))

NODE_LABEL_SCHEMA = pa.schema([("node_id", pa.int64())])

ADJACENCY_BY_SOURCE_SCHEMA = pa.schema(
    [
        ("src_id", pa.int64()),
        ("dst_id", pa.int64()),
        ("edge_id", pa.int64()),
    ]
)

_EDGE_RANGE_PATH = re.compile(
    r"data/edges/range-([1-9][0-9]*)/([0-9]+)/snapshot-[0-9]+(?:-[0-9a-f]{32})?\.parquet"
)
_DEFAULT_EDGE_RANGE_SIZE = 32_768
NODE_RANGE_SIZE = 8192


def node_records_from_table(table: pa.Table) -> list[NodeRecord]:
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
                properties=property_codec.loads(properties_json),
            )
        )
    return records


def edge_records_from_table(table: pa.Table) -> list[EdgeRecord]:
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
                properties=property_codec.loads(properties_json),
            )
        )
    return records


class GraphRepository:
    """Persist and load graph records from Parquet files."""

    def __init__(
        self,
        layout: DatabaseLayout,
        parquet_adapter: ParquetAdapter | None = None,
        tabular_adapter: PyArrowTabularAdapter | None = None,
        filesystem: FileSystemAdapter | None = None,
        *,
        edge_parquet_range_size: int | None = None,
    ) -> None:
        """Bind the layout and adapters used to read and publish canonical and derived datasets."""
        self._layout = layout
        self._filesystem = filesystem or FileSystemAdapter()
        self._parquet = parquet_adapter or ParquetAdapter()
        self._tabular = tabular_adapter or PyArrowTabularAdapter()
        self._edge_parquet_range_size = edge_parquet_range_size

    def prepare_edge_property_patch(
        self,
        snapshot: SnapshotManifest,
        *,
        edge_id: int,
        rel_type: str | None,
        properties: dict[str, object],
    ) -> tuple[bool, EdgePropertyPatch | None]:
        """Read only the touched range; decline edits that could change a global schema."""
        layout = _edge_range_layout(snapshot.edges)
        if layout is None:
            return False, None
        width, files = layout
        bucket = (edge_id - 1) // width
        canonical_file = files.get(bucket)
        if canonical_file is None:
            return True, None
        table = self.load_edge_table((canonical_file,))
        positions = pc.indices_nonzero(pc.equal(table["edge_id"], edge_id)).to_pylist()
        if not positions:
            return True, None
        if len(positions) != 1:
            return False, None
        position = positions[0]
        before = self.edge_records_from_table(table.slice(position, 1))[0]
        if rel_type is not None and before.rel_type != rel_type:
            return True, None
        if not _can_patch_properties(before, properties):
            return False, None
        candidates = [
            file
            for file in snapshot.edge_properties
            if _rel_type_from_edge_property_file(file) == before.rel_type
            and _edge_property_file_matches_layout(file, width)
            and Path(file.path).parent.name == f"{bucket:020d}"
        ]
        if len(candidates) != 1:
            return False, None
        property_file = candidates[0]
        projection = self._parquet.read_table(self._absolute_location(property_file.path))
        if any(
            key not in projection.schema.names or key in _EDGE_ENDPOINT_FIELDS for key in properties
        ):
            return False, None
        projected_positions = pc.indices_nonzero(
            pc.equal(projection["edge_id"], edge_id)
        ).to_pylist()
        if len(projected_positions) != 1:
            return False, None
        after = EdgeRecord(
            before.edge_id,
            before.rel_type,
            before.src_id,
            before.dst_id,
            {**before.properties, **properties},
        )
        try:
            row = _edge_property_table([after], projection.schema)
        except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError):
            return False, None
        canonical_row = table.slice(position, 1).set_column(
            table.schema.get_field_index("properties_json"),
            "properties_json",
            pa.array([property_codec.dumps(after.properties)], type=pa.string()),
        )
        return True, EdgePropertyPatch(
            before,
            after,
            canonical_file,
            _replace_row(table, position, canonical_row),
            property_file,
            _replace_row(projection, projected_positions[0], row),
        )

    def persist_edge_property_patch(
        self, patch: EdgePropertyPatch, snapshot_id: SnapshotId
    ) -> tuple[DatasetFile, DatasetFile]:
        """Write two immutable replacements without rebuilding unrelated graph datasets."""
        filename = f"snapshot-{snapshot_id.value:06d}-{uuid4().hex}.parquet"
        canonical_parts = (*Path(patch.canonical_file.path).parts[2:-1], filename)
        canonical = self._write_edge_file(patch.canonical_table, canonical_parts)
        property_parts = (*Path(patch.property_file.path).parts[2:-1], filename)
        projected = self._write_edge_property_table(patch.property_table, property_parts)
        return canonical, projected

    def prepare_node_property_patch(
        self,
        snapshot: SnapshotManifest,
        *,
        node_id: int,
        labels: tuple[str, ...],
        properties: dict[str, object],
    ) -> tuple[bool, NodePropertyPatch | None]:
        """Read the canonical node range and only the projections the edit can change."""
        files = _node_range_files(snapshot.nodes)
        if not files and snapshot.nodes:
            return False, None
        bucket = (node_id - 1) // NODE_RANGE_SIZE
        canonical = files.get(bucket)
        if canonical is None:
            return True, None
        table = self.load_node_table((canonical,))
        positions = pc.indices_nonzero(pc.equal(table["node_id"], node_id))
        if not len(positions):
            return True, None
        if len(positions) != 1:
            return False, None
        position = positions[0].as_py()
        before = node_records_from_table(table.slice(position, 1))[0]
        if not set(labels).issubset(before.labels):
            return True, None
        if not _can_patch_properties(before, properties):
            return False, None
        after = replace(before, properties={**before.properties, **properties})
        if before == after:
            return True, NodePropertyPatch(before, after, canonical, table, ())

        by_label = self.group_node_property_files(snapshot.node_properties)
        patches: list[tuple[DatasetFile, pa.Table]] = []
        for label in dict.fromkeys(before.labels):
            projection = _node_range_files(by_label.get(label, ())).get(bucket)
            if projection is None:
                return False, None
            projected = self._parquet.read_table(self._absolute_location(projection.path))
            positions = pc.indices_nonzero(pc.equal(projected["node_id"], node_id))
            if len(positions) != 1:
                return False, None
            index = positions[0].as_py()
            row = projected.slice(index, 1)
            for name, value in properties.items():
                column = projected.schema.get_field_index(name)
                if column < 0 or name == "node_id":
                    return False, None
                field = projected.schema.field(column)
                if (field.metadata or {}).get(b"grapharrow.encoding") != b"native":
                    return False, None
                try:
                    array = pa.array([value], type=field.type)
                except (pa.ArrowException, OverflowError):
                    return False, None
                row = row.set_column(column, field, array)
            patches.append((projection, _replace_row(projected, index, row)))

        replaced = _replace_row(table, position, _nodes_table([after]))
        return True, NodePropertyPatch(before, after, canonical, replaced, tuple(patches))

    def persist_node_property_patch(
        self, patch: NodePropertyPatch, snapshot_id: SnapshotId
    ) -> dict[DatasetFile, DatasetFile]:
        """Publish immutable replacements for a canonical range and its label projections."""
        filename = f"snapshot-{snapshot_id.value:06d}-{uuid4().hex}.parquet"
        bucket = (patch.after.node_id - 1) // NODE_RANGE_SIZE
        replacements = {
            patch.canonical_file: self._write_node_range(
                patch.canonical_table, self._layout.nodes_dir, bucket, filename, "nodes"
            )
        }
        for file, projection in patch.projections:
            label = _label_from_node_property_file(file)
            assert label is not None
            directory = self._join(self._layout.node_properties_dir, label)
            replacements[file] = self._write_node_range(
                projection, directory, bucket, filename, "node_properties"
            )
        return replacements

    @staticmethod
    def node_cache_files(files: tuple[DatasetFile, ...]) -> tuple[DatasetFile, ...]:
        return files

    def load_node_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        """Load node tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=NODE_SCHEMA,
        )

    def load_edge_table(
        self, files: tuple[DatasetFile, ...], *, topology_only: bool = False
    ) -> pa.Table:
        """Load edge tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=EDGE_TOPOLOGY_SCHEMA if topology_only else EDGE_SCHEMA,
        )

    @staticmethod
    def edge_property_sources(files: tuple[DatasetFile, ...]) -> None:
        return None

    def load_node_label_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        """Load node label index tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=NODE_LABEL_SCHEMA,
        )

    @staticmethod
    def adjacency_files(
        files: tuple[DatasetFile, ...], *, rel_type: str
    ) -> tuple[DatasetFile, ...]:
        """Select native adjacency references for a relationship type."""
        return tuple(file for file in files if _adjacency_rel_type_from_file(file) == rel_type)

    @staticmethod
    def has_complete_adjacency(
        snapshot: SnapshotManifest, rel_type: str, *, incoming: bool
    ) -> bool:
        """Check native coverage before enabling a selective derived read."""
        files = snapshot.adjacency_by_dest if incoming else snapshot.adjacency_by_source
        if sum(file.row_count for file in files) != sum(file.row_count for file in snapshot.edges):
            return False
        names = {_adjacency_rel_type_from_file(file) for file in files}
        return None not in names and rel_type in names and f"`{rel_type}`" not in names

    def load_adjacency_by_source_table(
        self, files: tuple[DatasetFile, ...], *, predicates: tuple[ScanPredicate, ...] = ()
    ) -> pa.Table:
        """Load adjacency-by-source tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=ADJACENCY_BY_SOURCE_SCHEMA,
            predicates=predicates,
        )

    def load_adjacency_by_dest_table(
        self, files: tuple[DatasetFile, ...], *, predicates: tuple[ScanPredicate, ...] = ()
    ) -> pa.Table:
        """Load adjacency-by-destination tables referenced by ``files`` into one Arrow table."""

        return self._parquet.read_dataset_table(
            [self._absolute_location(dataset_file.path) for dataset_file in files],
            schema=ADJACENCY_BY_SOURCE_SCHEMA,
            predicates=predicates,
        )

    @staticmethod
    def node_property_files(
        files: tuple[DatasetFile, ...], *, label: str
    ) -> tuple[DatasetFile, ...]:
        """Select the immutable property-projection files for a label."""
        return tuple(item for item in files if _label_from_node_property_file(item) == label)

    @staticmethod
    def group_node_property_files(
        files: tuple[DatasetFile, ...],
    ) -> dict[str, tuple[DatasetFile, ...]]:
        """Group property-projection files by their manifest label."""
        by_label: dict[str, list[DatasetFile]] = {}
        for item in files:
            label = _label_from_node_property_file(item)
            if label is not None:
                by_label.setdefault(label, []).append(item)
        return {label: tuple(items) for label, items in by_label.items()}

    def load_node_property_projection(
        self, files: tuple[DatasetFile, ...], *, label: str
    ) -> pa.Table:
        """Load the full, unfiltered property projection for a shared snapshot lease."""
        return self.load_node_property_table(files, label=label)

    def load_node_property_table(
        self,
        files: tuple[DatasetFile, ...],
        *,
        label: str,
        columns: list[str] | None = None,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table:
        """Load the property-projection dataset for ``label``."""

        return self._parquet.read_dataset_table(
            [
                self._absolute_location(item.path)
                for item in self.node_property_files(files, label=label)
            ],
            columns=columns,
            predicates=predicates,
        )

    def iter_node_property_batches(
        self,
        files: tuple[DatasetFile, ...],
        *,
        label: str,
        columns: list[str],
        predicates: tuple[ScanPredicate, ...] = (),
        batch_size: int = 65536,
    ) -> Generator[pa.RecordBatch, None, None]:
        """Stream a property projection from one immutable snapshot."""
        return self._parquet.iter_dataset_batches(
            [
                self._absolute_location(item.path)
                for item in self.node_property_files(files, label=label)
            ],
            columns=columns,
            predicates=predicates,
            batch_size=batch_size,
        )

    def load_node_property_schema(self, files: tuple[DatasetFile, ...], *, label: str) -> pa.Schema:
        """Inspect the per-label projection before selecting columns or predicates."""
        return self._parquet.read_dataset_schema(
            [
                self._absolute_location(item.path)
                for item in self.node_property_files(files, label=label)
            ]
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

    node_records_from_table = staticmethod(node_records_from_table)
    edge_records_from_table = staticmethod(edge_records_from_table)

    def persist_snapshot(
        self,
        *,
        snapshot_id: SnapshotId,
        nodes: list[NodeRecord],
        edges: list[EdgeRecord],
        previous_snapshot: SnapshotManifest | None = None,
        previous_nodes: tuple[NodeRecord, ...] | None = None,
        previous_edges: tuple[EdgeRecord, ...] | None = None,
        previous_edge_table: pa.Table | None = None,
        edge_table: pa.Table | None = None,
    ) -> PersistedGraph:
        """Write graph records, optionally reusing the canonical table for ``edges``."""

        self._layout.ensure_base_directories()
        filename = f"snapshot-{snapshot_id.value:06d}-{uuid4().hex}.parquet"
        same_nodes = (
            previous_snapshot is not None
            and previous_nodes is not None
            and list(previous_nodes) == nodes
        )
        relationship_types = {edge.rel_type for edge in edges}
        edge_groups = _unchanged_edge_groups(previous_edges, edges)
        same_edges = (
            previous_snapshot is not None
            and previous_edges is not None
            and list(previous_edges) == edges
            and edge_groups == relationship_types
        )
        node_files = (
            previous_snapshot.nodes
            if same_nodes and previous_snapshot is not None
            else self._persist_node_files(
                filename,
                nodes,
                previous_nodes or (),
                previous_snapshot.nodes if previous_snapshot is not None else (),
            )
        )
        previous_edge_files = previous_snapshot.edges if previous_snapshot is not None else ()
        edge_files = previous_edge_files
        if not same_edges:
            if edge_table is None:
                edge_table = _edges_table(edges)
            edge_files = self._persist_edge_files(
                filename,
                edge_table,
                previous_edge_files=previous_edge_files,
                previous_edge_table=previous_edge_table,
                allow_conversion=previous_edges is None,
            )
        label_groups = _unchanged_label_groups(previous_nodes, nodes)
        topology_groups = _unchanged_topology_groups(previous_edges, edges)

        node_label_files = self._persist_node_label_indexes(
            filename=filename,
            nodes=nodes,
            reuse_files=previous_snapshot.node_labels if previous_snapshot is not None else (),
            unchanged=label_groups,
        )
        node_property_files = self._persist_node_property_tables(
            filename=filename,
            nodes=nodes,
            previous_nodes=previous_nodes or (),
            reuse_files=previous_snapshot.node_properties if previous_snapshot is not None else (),
        )
        adjacency_files, adjacency_by_dest_files = self._persist_adjacency(
            filename=filename,
            relationship_types=relationship_types,
            edge_table=edge_table,
            previous_snapshot=previous_snapshot,
            unchanged=topology_groups,
        )
        edge_property_files = self._persist_edge_property_tables(
            filename=filename,
            edges=edges,
            edge_files=edge_files,
            previous_snapshot=previous_snapshot,
            unchanged=edge_groups,
        )

        persisted = PersistedGraph(
            nodes=node_files,
            edges=edge_files,
            node_labels=node_label_files,
            node_properties=node_property_files,
            adjacency_by_source=adjacency_files,
            adjacency_by_dest=adjacency_by_dest_files,
            edge_properties=edge_property_files,
        )
        return persisted

    def _persist_node_files(
        self,
        filename: str,
        nodes: list[NodeRecord],
        previous_nodes: tuple[NodeRecord, ...],
        previous_files: tuple[DatasetFile, ...],
    ) -> tuple[DatasetFile, ...]:
        files = _node_range_files(previous_files)
        prior = _group_node_ranges(previous_nodes) if files else {}
        ranges = _group_node_ranges(nodes) or {0: []}
        return tuple(
            files[bucket]
            if bucket in files and prior.get(bucket) == records
            else self._write_node_range(
                _nodes_table(records), self._layout.nodes_dir, bucket, filename, "nodes"
            )
            for bucket, records in sorted(ranges.items())
        )

    def _write_node_range(
        self, table: pa.Table, directory: str | Path, bucket: int, filename: str, kind: DatasetKind
    ) -> DatasetFile:
        parts = (f"range-{NODE_RANGE_SIZE}", f"{bucket:020d}", filename) if bucket else (filename,)
        path = self._join(directory, *parts)
        self._filesystem.create_dir(self._parent(path))
        self._parquet.write_table(table, path)
        bounds = pc.min_max(table["node_id"]).as_py()
        return _dataset_file_for(
            path=path,
            layout=self._layout,
            filesystem=self._filesystem,
            kind=kind,
            row_count=table.num_rows,
            min_key=bounds["min"],
            max_key=bounds["max"],
        )

    def _persist_edge_files(
        self,
        filename: str,
        table: pa.Table,
        *,
        previous_edge_files: tuple[DatasetFile, ...],
        previous_edge_table: pa.Table | None,
        allow_conversion: bool,
    ) -> tuple[DatasetFile, ...]:
        """Retain existing ranges and write one immutable file per changed range."""
        previous_layout = _edge_range_layout(previous_edge_files)
        previous_files: dict[int, DatasetFile]
        if allow_conversion and self._edge_parquet_range_size is not None:
            width, previous_files = self._edge_parquet_range_size, {}
        elif previous_layout is not None:
            width, previous_files = previous_layout
        elif allow_conversion or not previous_edge_files:
            width = (
                self._edge_parquet_range_size
                if self._edge_parquet_range_size is not None
                else _DEFAULT_EDGE_RANGE_SIZE
            )
            previous_files = {}
        else:
            width, previous_files = 0, {}

        if not width:
            return (self._write_edge_file(table, (filename,)),)

        previous_ranges = (
            dict(_edge_ranges(previous_edge_table, width))
            if previous_files and previous_edge_table is not None
            else {}
        )
        files = []
        for bucket, rows in _edge_ranges(table, width):
            previous_file = previous_files.get(bucket)
            previous_rows = previous_ranges.get(bucket)
            if (
                previous_file is not None
                and previous_rows is not None
                and previous_file.row_count == rows.num_rows
                and previous_rows.equals(rows)
            ):
                files.append(previous_file)
            else:
                parts = (f"range-{width}", f"{bucket:020d}", filename)
                files.append(self._write_edge_file(rows, parts))
        return tuple(files)

    def _write_edge_file(self, table: pa.Table, parts: tuple[str, ...]) -> DatasetFile:
        final_path = self._join(self._layout.edges_dir, *parts)
        self._filesystem.create_dir(self._parent(final_path))
        self._parquet.write_table(table, final_path)
        bounds = pc.min_max(table["edge_id"]).as_py()
        return _dataset_file_for(
            path=final_path,
            layout=self._layout,
            filesystem=self._filesystem,
            kind="edges",
            row_count=table.num_rows,
            min_key=bounds["min"],
            max_key=bounds["max"],
        )

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
        filename: str,
        nodes: list[NodeRecord],
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
                filename,
            )
            self._filesystem.create_dir(self._parent(final_path))
            node_ids = sorted(node_ids)
            table = pa.Table.from_pydict({"node_id": node_ids}, schema=NODE_LABEL_SCHEMA)
            self._parquet.write_table(table, final_path)
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

    def _persist_adjacency(
        self,
        *,
        filename: str,
        relationship_types: set[str],
        edge_table: pa.Table | None,
        previous_snapshot: SnapshotManifest | None,
        unchanged: set[str],
    ) -> tuple[tuple[DatasetFile, ...], tuple[DatasetFile, ...]]:
        """Reuse intact indexes and build missing directions from canonical Arrow data."""
        by_source: dict[str, list[DatasetFile]] = {}
        by_dest: dict[str, list[DatasetFile]] = {}
        if previous_snapshot is not None:
            for files, grouped in (
                (previous_snapshot.adjacency_by_source, by_source),
                (previous_snapshot.adjacency_by_dest, by_dest),
            ):
                for dataset in files:
                    rel_type = Path(dataset.path).parent.name
                    if rel_type in unchanged:
                        grouped.setdefault(rel_type, []).append(dataset)

        missing = relationship_types - (by_source.keys() & by_dest.keys())
        if missing:
            if edge_table is None:
                # Older manifests may lack a direction even when canonical edges are unchanged.
                assert previous_snapshot is not None
                edge_table = self.load_edge_table(previous_snapshot.edges)
            if missing != relationship_types:
                edge_table = edge_table.select(
                    [*ADJACENCY_BY_SOURCE_SCHEMA.names, "rel_type"]
                ).filter(pc.is_in(edge_table["rel_type"], value_set=pa.array(list(missing))))
            for rel_type, endpoints in _adjacency_rows_by_type(edge_table, missing):
                if rel_type not in by_source:
                    by_source[rel_type] = [
                        self._write_adjacency(endpoints, rel_type, filename, by_source=True)
                    ]
                if rel_type not in by_dest:
                    by_dest[rel_type] = [
                        self._write_adjacency(endpoints, rel_type, filename, by_source=False)
                    ]
        ordered_types = sorted(relationship_types)
        return (
            tuple(dataset for rel_type in ordered_types for dataset in by_source[rel_type]),
            tuple(dataset for rel_type in ordered_types for dataset in by_dest[rel_type]),
        )

    def _write_adjacency(
        self,
        endpoints: pa.Table,
        rel_type: str,
        filename: str,
        *,
        by_source: bool,
    ) -> DatasetFile:
        primary, secondary = ("src_id", "dst_id") if by_source else ("dst_id", "src_id")
        final_dir = (
            self._layout.adjacency_by_source_dir
            if by_source
            else self._layout.adjacency_by_dest_dir
        )
        final_path = self._join(final_dir, rel_type, filename)
        self._filesystem.create_dir(self._parent(final_path))
        table = endpoints.sort_by(
            [(primary, "ascending"), (secondary, "ascending"), ("edge_id", "ascending")]
        )
        self._parquet.write_table(table, final_path)
        keys = table[primary]
        return _dataset_file_for(
            path=final_path,
            layout=self._layout,
            filesystem=self._filesystem,
            kind="adjacency_by_source" if by_source else "adjacency_by_dest",
            row_count=table.num_rows,
            min_key=keys[0].as_py(),
            max_key=keys[-1].as_py(),
        )

    def _persist_node_property_tables(
        self,
        *,
        filename: str,
        nodes: list[NodeRecord],
        previous_nodes: tuple[NodeRecord, ...],
        reuse_files: tuple[DatasetFile, ...] = (),
    ) -> tuple[DatasetFile, ...]:
        """Retain unchanged ranges with the same per-label property schema."""
        dataset_files: list[DatasetFile] = []
        previous_labels = _group_nodes_by_label(previous_nodes)
        existing = self.group_node_property_files(reuse_files)
        for label, typed_nodes in sorted(_group_nodes_by_label(nodes).items()):
            reused = existing.get(label, ())
            prior_nodes = previous_labels.get(label, [])
            if reused and prior_nodes == typed_nodes:
                dataset_files.extend(reused)
                continue
            directory = self._join(self._layout.node_properties_dir, label)
            table = _node_property_table(sorted(typed_nodes, key=lambda node: node.node_id))
            files = _node_range_files(reused)
            reusable: set[int] = set()
            if files:
                prior = _group_node_ranges(prior_nodes)
                reusable = {
                    bucket
                    for bucket, records in _group_node_ranges(typed_nodes).items()
                    if bucket in files and prior.get(bucket) == records
                }
                if reusable and not table.schema.equals(
                    self._parquet.read_dataset_schema([self._absolute_location(reused[0].path)]),
                    check_metadata=True,
                ):
                    reusable.clear()
            dataset_files.extend(
                files[bucket]
                if bucket in reusable
                else self._write_node_range(rows, directory, bucket, filename, "node_properties")
                for bucket, rows in _id_ranges(table, "node_id", NODE_RANGE_SIZE)
            )
        return tuple(dataset_files)

    def _persist_edge_property_tables(
        self,
        *,
        filename: str,
        edges: list[EdgeRecord],
        edge_files: tuple[DatasetFile, ...],
        previous_snapshot: SnapshotManifest | None,
        unchanged: set[str],
    ) -> tuple[DatasetFile, ...]:
        """Write schema-consistent property projections on the canonical edge ranges."""
        dataset_files: list[DatasetFile] = []
        by_type: dict[str, list[EdgeRecord]] = {}
        for edge in edges:
            by_type.setdefault(edge.rel_type, []).append(edge)

        previous_by_type: dict[str, list[DatasetFile]] = {}
        if previous_snapshot is not None:
            for file in previous_snapshot.edge_properties:
                rel = _rel_type_from_edge_property_file(file)
                if rel is not None:
                    previous_by_type.setdefault(rel, []).append(file)

        layout = _edge_range_layout(edge_files)
        width, ranges = layout if layout is not None else (0, {})
        previous_edges = set(previous_snapshot.edges) if previous_snapshot is not None else set()
        reusable_buckets = {bucket for bucket, file in ranges.items() if file in previous_edges}

        for rel_type, typed_edges in sorted(by_type.items()):
            reused = previous_by_type.get(rel_type, [])
            if (
                rel_type in unchanged
                and reused
                and all(_edge_property_file_matches_layout(file, width) for file in reused)
            ):
                dataset_files.extend(reused)
                continue
            sorted_edges = sorted(typed_edges, key=lambda item: item.edge_id)
            schema = _edge_property_schema(sorted_edges)
            if not width:
                dataset_files.append(
                    self._write_edge_property_file((rel_type, filename), sorted_edges, schema)
                )
                continue

            # A changed field layout invalidates every fragment for this relationship type.
            signature = hashlib.sha256(schema.serialize()).hexdigest()
            directory = f"range-{width}-{signature}"
            prior = {Path(file.path).parent: file for file in reused}
            for bucket, records in _group_edge_records(sorted_edges, width):
                parts = (rel_type, directory, f"{bucket:020d}")
                old = prior.get(Path("derived", "edge_properties", *parts))
                if old is not None and bucket in reusable_buckets and old.row_count == len(records):
                    dataset_files.append(old)
                else:
                    dataset_files.append(
                        self._write_edge_property_file((*parts, filename), records, schema)
                    )
        return tuple(dataset_files)

    def _write_edge_property_file(
        self,
        parts: tuple[str, ...],
        records: list[EdgeRecord],
        schema: pa.Schema,
    ) -> DatasetFile:
        return self._write_edge_property_table(_edge_property_table(records, schema), parts)

    def _write_edge_property_table(self, table: pa.Table, parts: tuple[str, ...]) -> DatasetFile:
        final_path = self._join(self._layout.edge_properties_dir, *parts)
        self._filesystem.create_dir(self._parent(final_path))
        self._parquet.write_table(table, final_path)
        bounds = pc.min_max(table["edge_id"]).as_py()
        return _dataset_file_for(
            path=final_path,
            layout=self._layout,
            filesystem=self._filesystem,
            kind="edge_properties",
            row_count=table.num_rows,
            min_key=bounds["min"],
            max_key=bounds["max"],
        )


def _replace_row(table: pa.Table, position: int, replacement: pa.Table) -> pa.Table:
    """Reuse Arrow buffers outside one edited row."""
    return pa.concat_tables([table.slice(0, position), replacement, table.slice(position + 1)])


def _can_patch_properties(entity: EdgeRecord | NodeRecord, properties: dict[str, object]) -> bool:
    return all(
        type(value) in (str, bool, int, float)
        and type(entity.properties.get(name)) is type(value)
        and (type(value) is not int or -(2**63) <= value < 2**63)
        for name, value in properties.items()
    )


def _edge_property_file_matches_layout(file: DatasetFile, width: int) -> bool:
    parts = Path(file.path).parts
    if not width:
        return len(parts) == 4
    return len(parts) == 6 and parts[3].startswith(f"range-{width}-")


def _group_edge_records(
    records: list[EdgeRecord], width: int
) -> Generator[tuple[int, list[EdgeRecord]], None, None]:
    start = 0
    bucket = (records[0].edge_id - 1) // width
    for position, edge in enumerate(records):
        current = (edge.edge_id - 1) // width
        if current != bucket:
            yield bucket, records[start:position]
            start, bucket = position, current
    yield bucket, records[start:]


def _edge_range_layout(files: tuple[DatasetFile, ...]) -> tuple[int, dict[int, DatasetFile]] | None:
    """Read the range width and bucket IDs from canonical file paths."""
    width = 0
    buckets: dict[int, DatasetFile] = {}
    for file in files:
        match = _EDGE_RANGE_PATH.fullmatch(Path(file.path).as_posix())
        if match is None:
            return None
        size, bucket = map(int, match.groups())
        if (width and size != width) or bucket in buckets:
            return None
        width = size
        buckets[bucket] = file
    return (width, buckets) if width else None


def _edge_ranges(table: pa.Table, width: int) -> Generator[tuple[int, pa.Table], None, None]:
    """Slice canonical rows by stable, one-based edge ID ranges."""
    yield from _id_ranges(table, "edge_id", width)


def _id_ranges(
    table: pa.Table, id_column: str, width: int
) -> Generator[tuple[int, pa.Table], None, None]:
    if not table.num_rows:
        # An empty file can retain the layout after the last row is removed.
        yield 0, table
        return
    groups = pc.divide(pc.subtract(table[id_column], 1), width)
    if pc.any(pc.less(groups.slice(1), groups.slice(0, len(groups) - 1))).as_py():
        order = pc.sort_indices(groups)
        table = table.take(order)
        groups = pc.take(groups, order)
    boundaries = pc.indices_nonzero(
        pc.not_equal(groups.slice(0, len(groups) - 1), groups.slice(1))
    ).to_pylist()
    start = 0
    for stop in [*(index + 1 for index in boundaries), table.num_rows]:
        yield groups[start].as_py(), table.slice(start, stop - start)
        start = stop


def _group_nodes_by_label(nodes: Iterable[NodeRecord]) -> dict[str, list[NodeRecord]]:
    groups: dict[str, list[NodeRecord]] = {}
    for node in nodes:
        for label in node.labels:
            groups.setdefault(label, []).append(node)
    return groups


def _group_node_ranges(nodes: Iterable[NodeRecord]) -> dict[int, list[NodeRecord]]:
    groups: dict[int, list[NodeRecord]] = {}
    for node in nodes:
        groups.setdefault((node.node_id - 1) // NODE_RANGE_SIZE, []).append(node)
    return groups


def _node_range_files(files: tuple[DatasetFile, ...]) -> dict[int, DatasetFile]:
    ranges: dict[int, DatasetFile] = {}
    for file in files:
        parts = Path(file.path).parts
        if len(parts) >= 5 and parts[-3] == f"range-{NODE_RANGE_SIZE}" and parts[-2].isdigit():
            bucket = int(parts[-2])
        elif len(parts) == (3 if file.kind == "nodes" else 4) and (
            not file.row_count
            or (
                file.min_key is not None
                and file.max_key is not None
                and 1 <= file.min_key <= file.max_key <= NODE_RANGE_SIZE
            )
        ):
            bucket = 0
        else:
            return {}
        ranges[bucket] = file
    return ranges if len(ranges) == len(files) else {}


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
    previous: tuple[EdgeRecord, ...] | None,
    current: list[EdgeRecord],
) -> set[str]:
    before: dict[str, list[EdgeRecord]] = {}
    after: dict[str, list[EdgeRecord]] = {}
    for edge in previous or ():
        before.setdefault(edge.rel_type, []).append(edge)
    for edge in current:
        after.setdefault(edge.rel_type, []).append(edge)
    unchanged = set()
    for kind, records in after.items():
        old = before.get(kind)
        if old is None or old != records:
            continue
        if any(
            left is not right
            and property_codec.dumps(left.properties) != property_codec.dumps(right.properties)
            for left, right in zip(old, records, strict=True)
        ):
            continue
        unchanged.add(kind)
    return unchanged


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


def _adjacency_rows_by_type(
    edges: pa.Table, relationship_types: set[str]
) -> Generator[tuple[str, pa.Table], None, None]:
    """Group topology columns once so both directions can sort the same rows."""
    columns = ADJACENCY_BY_SOURCE_SCHEMA.names
    if len(relationship_types) == 1:
        yield next(iter(relationship_types)), edges.select(columns)
        return
    rows = edges.select([*columns, "rel_type"]).sort_by([("rel_type", "ascending")])
    kinds = rows["rel_type"]
    boundaries = pc.indices_nonzero(
        pc.not_equal(kinds.slice(0, len(kinds) - 1), kinds.slice(1))
    ).to_pylist()
    endpoints = rows.select(columns)
    start = 0
    for stop in [*(index + 1 for index in boundaries), rows.num_rows]:
        yield kinds[start].as_py(), endpoints.slice(start, stop - start)
        start = stop


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
                [property_codec.dumps(record.properties) for record in records],
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
                [property_codec.dumps(record.properties) for record in records],
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
    fields = [
        pa.field(
            "node_id",
            pa.int64(),
            metadata={"grapharrow.encoding": "reserved-collision"}
            if "node_id" in property_names
            else None,
        )
    ]
    for property_name in property_names:
        if property_name == "node_id":
            continue
        values = [record.properties.get(property_name) for record in records]
        array = _property_column_array(values)
        columns[property_name] = array
        types = {type(value) for value in values if value is not None}
        encoding = "native" if len(types) <= 1 and types <= {str, bool, int, float} else "json"
        if types and types <= {int, float} and len(types) > 1:
            encoding = "lossy"
        fields.append(
            pa.field(property_name, array.type, metadata={"grapharrow.encoding": encoding})
        )
    return pa.Table.from_arrays(list(columns.values()), schema=pa.schema(fields))


_EDGE_ENDPOINT_FIELDS = ("edge_id", "src_id", "dst_id")


def _edge_property_schema(records: list[EdgeRecord]) -> pa.Schema:
    """Choose one encoding per property over the entire relationship type."""
    observed: dict[str, set[type[object]]] = {}
    for record in records:
        for name, value in record.properties.items():
            kinds = observed.setdefault(name, set())
            if value is not None:
                kind = type(value)
                if kind is int and not -(2**63) <= cast(int, value) < 2**63:
                    kind = object
                kinds.add(kind)

    fields = [pa.field(name, pa.int64()) for name in _EDGE_ENDPOINT_FIELDS]
    native = {bool: pa.bool_(), int: pa.int64(), float: pa.float64(), str: pa.string()}
    for name in sorted(observed.keys() - set(_EDGE_ENDPOINT_FIELDS)):
        kinds = observed[name]
        if not kinds:
            fields.append(pa.field(name, pa.null()))
        elif len(kinds) == 1 and (kind := next(iter(kinds))) in native:
            fields.append(pa.field(name, native[kind]))
        else:
            fields.append(pa.field(name, pa.string(), metadata={"grapharrow.encoding": "json"}))
    return pa.schema(fields)


def _edge_property_table(records: list[EdgeRecord], schema: pa.Schema) -> pa.Table:
    """Build one projected file with the shared relationship-type schema."""
    columns = [
        pa.array([record.edge_id for record in records], type=pa.int64()),
        pa.array([record.src_id for record in records], type=pa.int64()),
        pa.array([record.dst_id for record in records], type=pa.int64()),
    ]
    for field in list(schema)[len(_EDGE_ENDPOINT_FIELDS) :]:
        values = [record.properties.get(field.name) for record in records]
        if field.metadata:
            values = [
                property_codec.dumps(value) if value is not None else None for value in values
            ]
        columns.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(columns, schema=schema)


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
        [property_codec.dumps(value) if value is not None else None for value in values],
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


def _adjacency_rel_type_from_file(dataset_file: DatasetFile) -> str | None:
    """Extract the relationship type encoded in an adjacency derived-dataset path."""
    parts = PurePosixPath(dataset_file.path).parts
    if (
        "adjacency" not in parts
        or ("by_source" not in parts and "by_dest" not in parts)
        or len(parts) < 2
    ):
        return None
    return parts[-2]


__all__ = ["EdgeRecord", "GraphRepository", "NodeRecord", "PersistedGraph"]
