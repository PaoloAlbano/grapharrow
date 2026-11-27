"""Arrow graph reads derived from an immutable Delta table version."""

from __future__ import annotations

from collections.abc import Callable, Generator, Iterable
from contextlib import AbstractContextManager, ExitStack
from dataclasses import dataclass
from functools import partial
from urllib.parse import quote, unquote

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]
import pyarrow.dataset as ds  # type: ignore[import-untyped]

from grapharrow.adapters.scan_filter import dataset_filter
from grapharrow.adapters.snapshot_tables import SnapshotTableCache
from grapharrow.core import property_codec
from grapharrow.domain import DatasetFile, SnapshotId, SnapshotManifest
from grapharrow.domain.records import EdgeRecord, NodeRecord
from grapharrow.domain.scan import ScanPredicate
from grapharrow.domain.snapshot import DatasetKind
from grapharrow.repositories.graph import (
    _EDGE_ENDPOINT_FIELDS,
    ADJACENCY_BY_SOURCE_SCHEMA,
    EDGE_SCHEMA,
    EDGE_TOPOLOGY_SCHEMA,
    NODE_LABEL_SCHEMA,
    NODE_SCHEMA,
    _edge_property_schema,
    _edge_property_table,
    _node_property_table,
    edge_records_from_table,
    node_records_from_table,
)
from grapharrow.storage.contracts import EdgePropertySource

_EDGE_RANGE_GROUP_SIZE = 32
_MIN_GROUPED_EDGE_RANGES = 4 * _EDGE_RANGE_GROUP_SIZE
_EDGE_PROPERTY_COLUMNS = ["rel_type", "properties_json"]


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


@dataclass(frozen=True, slots=True)
class DeltaEdgeRange:
    partition: str
    files: tuple[DatasetFile, ...]


class DeltaGraphStore:
    """Read canonical rows and derive projections from that same captured version."""

    def __init__(
        self,
        read_partition: Callable[[SnapshotId, str, list[str], object], pa.Table],
        *,
        edge_ranges: Callable[[SnapshotId], tuple[DeltaEdgeRange, ...]],
        node_files: Callable[[SnapshotId], tuple[DatasetFile, ...]],
        snapshot_tables: SnapshotTableCache,
        filter_pushdown_enabled: bool,
    ) -> None:
        self._read_partition = read_partition
        self._edge_ranges = edge_ranges
        self._node_files = node_files
        self._snapshot_tables = snapshot_tables
        self._filter_pushdown_enabled = filter_pushdown_enabled

    def node_cache_files(self, files: tuple[DatasetFile, ...]) -> tuple[DatasetFile, ...]:
        if not files:
            return ()
        ref = files[0]
        active = self._node_files(_snapshot_id(ref))
        if not active:
            return files
        if ref.kind == "node_properties":
            label = quote(_name(ref), safe="")
            return tuple(
                DatasetFile(f"{item.path}#{label}", item.row_count, "node_properties")
                for item in active
            )
        return active

    def load_node_table(self, files: tuple[DatasetFile, ...]) -> pa.Table:
        if not files:
            return NODE_SCHEMA.empty_table()
        return (
            self._read_partition(_snapshot_id(files[0]), "nodes", NODE_SCHEMA.names, None)
            .cast(NODE_SCHEMA)
            .sort_by([("node_id", "ascending")])
        )

    def load_edge_table(
        self, files: tuple[DatasetFile, ...], *, topology_only: bool = False
    ) -> pa.Table:
        schema = EDGE_TOPOLOGY_SCHEMA if topology_only else EDGE_SCHEMA
        if not files:
            return schema.empty_table()
        snapshot_id = _snapshot_id(files[0])
        ranges = self._edge_ranges(snapshot_id)
        if not ranges:
            return self._edge_rows(files[0], schema)
        if len(ranges) < _MIN_GROUPED_EDGE_RANGES or not self._snapshot_tables.max_bytes:
            return self._load_edge_ranges(snapshot_id, ranges, schema)
        with ExitStack() as leases:
            blocks = []
            for start in range(0, len(ranges), _EDGE_RANGE_GROUP_SIZE):
                group = ranges[start : start + _EDGE_RANGE_GROUP_SIZE]
                key = tuple(physical for item in group for physical in item.files)
                blocks.append(
                    leases.enter_context(
                        self._snapshot_tables.borrow(
                            (
                                "edge_topology_range_group"
                                if topology_only
                                else "edge_range_group",
                                key,
                            ),
                            partial(self._load_edge_ranges, snapshot_id, group, schema),
                        )
                    )
                )
            return pa.concat_tables(blocks)

    def _load_edge_ranges(
        self, snapshot_id: SnapshotId, ranges: tuple[DeltaEdgeRange, ...], schema: pa.Schema
    ) -> pa.Table:
        with ExitStack() as leases:
            return pa.concat_tables(
                [
                    leases.enter_context(self._borrow_edge_range(snapshot_id, item, schema))
                    for item in ranges
                ]
            )

    def edge_property_sources(
        self, files: tuple[DatasetFile, ...]
    ) -> Iterable[EdgePropertySource] | None:
        if not files:
            return ()
        snapshot_id = _snapshot_id(files[0])
        ranges = self._edge_ranges(snapshot_id)
        if not ranges:
            return None
        return (
            (item.files, partial(self._read_range_properties, snapshot_id, item)) for item in ranges
        )

    def _read_range_properties(self, snapshot_id: SnapshotId, item: DeltaEdgeRange) -> pa.Table:
        cached = self._snapshot_tables.project_cached(
            ("edge_range", item.files), _EDGE_PROPERTY_COLUMNS
        )
        if cached is not None:
            return cached
        return self._read_partition(snapshot_id, item.partition, _EDGE_PROPERTY_COLUMNS, None)

    def _borrow_edge_range(
        self, snapshot_id: SnapshotId, item: DeltaEdgeRange, schema: pa.Schema = EDGE_SCHEMA
    ) -> AbstractContextManager[pa.Table]:
        topology_only = schema == EDGE_TOPOLOGY_SCHEMA

        def load() -> pa.Table:
            if topology_only:
                cached = self._snapshot_tables.project_cached(
                    ("edge_range", item.files), schema.names
                )
                if cached is not None:
                    return cached
            return (
                self._read_partition(snapshot_id, item.partition, schema.names, None)
                .cast(schema)
                .sort_by([("edge_id", "ascending")])
            )

        return self._snapshot_tables.borrow(
            ("edge_topology_range" if topology_only else "edge_range", item.files), load
        )

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
            ("node_properties", self.node_cache_files(selected)),
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
        snapshot_id, rel_type = _snapshot_id(file), _name(file)
        ranges = self._edge_ranges(snapshot_id)
        if ranges:
            with ExitStack() as leases:
                tables = [
                    leases.enter_context(self._borrow_range_projection(snapshot_id, item, rel_type))
                    for item in ranges
                ]
                return _merge_edge_projections([table for table in tables if table.num_rows])
        return _edge_projection(self._edge_rows(file, EDGE_SCHEMA))

    def _borrow_range_projection(
        self, snapshot_id: SnapshotId, item: DeltaEdgeRange, rel_type: str
    ) -> AbstractContextManager[pa.Table]:
        suffix = quote(rel_type, safe="")
        files = tuple(
            DatasetFile(f"{file.path}#{suffix}", file.row_count, file.kind) for file in item.files
        )

        def load() -> pa.Table:
            with self._borrow_edge_range(snapshot_id, item) as edges:
                return _edge_projection(edges.filter(pc.equal(edges["rel_type"], rel_type)))

        return self._snapshot_tables.borrow(("edge_range_properties", files), load)

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


def _edge_projection(table: pa.Table) -> pa.Table:
    records = edge_records_from_table(table)
    return _edge_property_table(records, _edge_property_schema(records))


def _merge_edge_projections(tables: list[pa.Table]) -> pa.Table:
    if not tables:
        return _edge_projection(EDGE_SCHEMA.empty_table())
    endpoints = list(tables[0].schema)[: len(_EDGE_ENDPOINT_FIELDS)]
    properties: dict[str, pa.Field] = {}
    for table in tables:
        for field in list(table.schema)[len(endpoints) :]:
            previous = properties.get(field.name)
            if previous is None or pa.types.is_null(previous.type):
                properties[field.name] = field
            elif not pa.types.is_null(field.type) and not previous.equals(
                field, check_metadata=True
            ):
                properties[field.name] = pa.field(
                    field.name, pa.string(), metadata={"grapharrow.encoding": "json"}
                )
    schema = pa.schema([*endpoints, *(properties[name] for name in sorted(properties))])
    return pa.concat_tables([_align_edge_projection(table, schema) for table in tables])


def _align_edge_projection(table: pa.Table, schema: pa.Schema) -> pa.Table:
    if table.schema.equals(schema, check_metadata=True):
        return table
    arrays = []
    for target in schema:
        index = table.schema.get_field_index(target.name)
        if index == -1 or pa.types.is_null(table.schema.field(index).type):
            arrays.append(pa.nulls(table.num_rows, type=target.type))
            continue
        source = table.schema.field(index)
        column = table.column(index)
        if target.metadata and not source.metadata:
            column = pa.array(
                [
                    property_codec.dumps(value) if value is not None else None
                    for value in column.to_pylist()
                ],
                type=target.type,
            )
        arrays.append(column)
    return pa.Table.from_arrays(arrays, schema=schema)
