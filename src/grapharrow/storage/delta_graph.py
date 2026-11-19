"""Arrow graph reads derived from an immutable Delta table version."""

from __future__ import annotations

from collections.abc import Callable, Generator
from functools import partial
from urllib.parse import quote, unquote

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]
import pyarrow.dataset as ds  # type: ignore[import-untyped]

from grapharrow.adapters.scan_filter import dataset_filter
from grapharrow.adapters.snapshot_tables import SnapshotTableCache
from grapharrow.domain import DatasetFile, SnapshotId, SnapshotManifest
from grapharrow.domain.records import EdgeRecord, NodeRecord
from grapharrow.domain.scan import ScanPredicate
from grapharrow.domain.snapshot import DatasetKind
from grapharrow.repositories.graph import (
    ADJACENCY_BY_SOURCE_SCHEMA,
    EDGE_SCHEMA,
    NODE_LABEL_SCHEMA,
    NODE_SCHEMA,
    _edge_property_schema,
    _edge_property_table,
    _node_property_table,
    edge_records_from_table,
    node_records_from_table,
)


def dataset_ref(
    snapshot_id: SnapshotId, kind: DatasetKind, count: int, name: str = ""
) -> DatasetFile:
    """Identify a logical projection at one Delta version for the engine's caches."""
    return DatasetFile(
        path=f"delta/{snapshot_id.value}/{kind}/{quote(name, safe='')}",
        row_count=count,
        kind=kind,
    )


def _name(file: DatasetFile) -> str:
    return unquote(file.path.rsplit("/", 1)[1])


def _snapshot_id(file: DatasetFile) -> SnapshotId:
    return SnapshotId(int(file.path.split("/")[1]))


class DeltaGraphStore:
    """Read canonical rows and derive projections from that same captured version."""

    def __init__(
        self,
        read_partition: Callable[[SnapshotId, str, list[str], object], pa.Table],
        *,
        snapshot_tables: SnapshotTableCache,
        filter_pushdown_enabled: bool,
    ) -> None:
        self._read_partition = read_partition
        self._snapshot_tables = snapshot_tables
        self._filter_pushdown_enabled = filter_pushdown_enabled

    def load_node_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        if not files:
            return NODE_SCHEMA.empty_table()
        return (
            self._read_partition(_snapshot_id(files[0]), "nodes", NODE_SCHEMA.names, None)
            .cast(NODE_SCHEMA)
            .sort_by([("node_id", "ascending")])
        )

    def load_edge_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        if not files:
            return EDGE_SCHEMA.empty_table()
        return self._edge_rows(files[0], EDGE_SCHEMA)

    def _edge_rows(self, file: DatasetFile, schema: pa.Schema) -> pa.Table:
        rel_type = _name(file)
        return (
            self._read_partition(
                _snapshot_id(file),
                "edges",
                schema.names,
                ds.field("rel_type") == rel_type if rel_type else None,
            )
            .cast(schema)
            .sort_by([("edge_id", "ascending")])
        )

    def _nodes_for_label(self, file: DatasetFile, columns: list[str]) -> pa.Table:
        nodes = self._read_partition(_snapshot_id(file), "nodes", columns, None)
        labels = nodes["labels"]
        indices = pc.list_parent_indices(labels.combine_chunks())
        matches = pc.equal(pc.list_flatten(labels), _name(file))
        positions = pc.unique(indices.filter(matches))
        return nodes.take(positions).sort_by([("node_id", "ascending")])

    def load_node_label_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        tables = [
            self._nodes_for_label(file, ["node_id", "labels"]).select(["node_id"]) for file in files
        ]
        return pa.concat_tables(tables) if tables else NODE_LABEL_SCHEMA.empty_table()

    @staticmethod
    def adjacency_files(
        files: tuple[DatasetFile, ...], *, rel_type: str
    ) -> tuple[DatasetFile, ...]:
        return tuple(file for file in files if _name(file) == rel_type)

    @staticmethod
    def has_complete_adjacency(
        snapshot: SnapshotManifest, rel_type: str, *, incoming: bool
    ) -> bool:
        files = snapshot.adjacency_by_dest if incoming else snapshot.adjacency_by_source
        return (
            sum(file.row_count for file in files) == sum(file.row_count for file in snapshot.edges)
            and any(_name(file) == rel_type for file in files)
            and not any(_name(file) == f"`{rel_type}`" for file in files)
        )

    def load_adjacency_by_source_table(
        self, files: tuple[DatasetFile, ...], *, predicates: tuple[ScanPredicate, ...] = ()
    ) -> pa.Table:
        return self._load_adjacency(files, predicates, endpoint="src_id")

    def load_adjacency_by_dest_table(
        self, files: tuple[DatasetFile, ...], *, predicates: tuple[ScanPredicate, ...] = ()
    ) -> pa.Table:
        return self._load_adjacency(files, predicates, endpoint="dst_id")

    def _load_adjacency(
        self,
        files: tuple[DatasetFile, ...],
        predicates: tuple[ScanPredicate, ...],
        *,
        endpoint: str,
    ) -> pa.Table:
        schema = ADJACENCY_BY_SOURCE_SCHEMA
        expression = dataset_filter(schema, predicates) if self._filter_pushdown_enabled else None
        tables = []
        for file in files:
            selected = ds.field("rel_type") == _name(file)
            if expression is not None:
                selected = selected & expression
            table = self._read_partition(_snapshot_id(file), "edges", schema.names, selected).cast(
                schema
            )
            tables.append(table)
        if not tables:
            return schema.empty_table()
        return pa.concat_tables(tables).sort_by([(endpoint, "ascending"), ("edge_id", "ascending")])

    @staticmethod
    def node_property_files(
        files: tuple[DatasetFile, ...], *, label: str
    ) -> tuple[DatasetFile, ...]:
        return tuple(file for file in files if _name(file) == label)

    @staticmethod
    def group_node_property_files(
        files: tuple[DatasetFile, ...],
    ) -> dict[str, tuple[DatasetFile, ...]]:
        return {_name(file): (file,) for file in files}

    def load_node_property_projection(
        self, files: tuple[DatasetFile, ...], *, label: str
    ) -> pa.Table:
        selected = self.node_property_files(files, label=label)
        if not selected:
            return pa.table({})
        nodes = self._nodes_for_label(selected[0], NODE_SCHEMA.names)
        return _node_property_table(node_records_from_table(nodes))

    def load_node_property_table(
        self,
        files: tuple[DatasetFile, ...],
        *,
        label: str,
        columns: list[str] | None = None,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table:
        selected = self.node_property_files(files, label=label)
        if not selected:
            return pa.table({})
        with self._snapshot_tables.borrow(
            ("node_properties", selected),
            partial(self.load_node_property_projection, selected, label=label),
        ) as table:
            return self._select(table, columns, predicates)

    def iter_node_property_batches(
        self,
        files: tuple[DatasetFile, ...],
        *,
        label: str,
        columns: list[str],
        predicates: tuple[ScanPredicate, ...] = (),
        batch_size: int = 65536,
    ) -> Generator[pa.RecordBatch, None, None]:
        table = self.load_node_property_table(
            files, label=label, columns=columns, predicates=predicates
        )
        yield from table.to_batches(max_chunksize=batch_size)

    def load_node_property_schema(self, files: tuple[DatasetFile, ...], *, label: str) -> pa.Schema:
        return self.load_node_property_table(files, label=label).schema

    def load_edge_property_table(
        self,
        files: tuple[DatasetFile, ...],
        *,
        rel_type: str,
        columns: list[str] | None = None,
        predicates: tuple[ScanPredicate, ...] = (),
    ) -> pa.Table:
        selected = self.adjacency_files(files, rel_type=rel_type)
        if not selected:
            return pa.table({})
        endpoints = ADJACENCY_BY_SOURCE_SCHEMA.names
        if (
            columns is not None
            and all(column in endpoints for column in columns)
            and all(predicate.column in endpoints for predicate in predicates)
        ):
            with self._snapshot_tables.borrow(
                ("edge_endpoints", selected),
                partial(self._edge_rows, selected[0], ADJACENCY_BY_SOURCE_SCHEMA),
            ) as table:
                return self._select(table, columns, predicates)
        with self._snapshot_tables.borrow(
            ("edge_properties", selected),
            partial(self._load_edge_property_projection, selected[0]),
        ) as table:
            return self._select(table, columns, predicates)

    def _load_edge_property_projection(self, file: DatasetFile) -> pa.Table:
        edges = edge_records_from_table(self._edge_rows(file, EDGE_SCHEMA))
        return _edge_property_table(edges, _edge_property_schema(edges))

    def _select(
        self, table: pa.Table, columns: list[str] | None, predicates: tuple[ScanPredicate, ...]
    ) -> pa.Table:
        expression = (
            dataset_filter(table.schema, predicates) if self._filter_pushdown_enabled else None
        )
        if columns is None and expression is None:
            return table
        dataset = ds.dataset(table)
        try:
            return dataset.to_table(columns=columns, filter=expression)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
            if expression is None:
                raise
            return dataset.to_table(columns=columns)

    def load_nodes(self, files: tuple[DatasetFile, ...]) -> list[NodeRecord]:
        return node_records_from_table(self.load_node_table(files))

    def load_edges(self, files: tuple[DatasetFile, ...]) -> list[EdgeRecord]:
        return edge_records_from_table(self.load_edge_table(files))

    node_records_from_table = staticmethod(node_records_from_table)
    edge_records_from_table = staticmethod(edge_records_from_table)
