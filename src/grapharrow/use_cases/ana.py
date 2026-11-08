"""Native GraphArrow execution behind the ANA v0 tool protocol."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from copy import deepcopy
from threading import RLock
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from grapharrow.adapters.adjacency import GraphAdjacency
from grapharrow.ana.catalog import SUPPORTED_ANA_TOOLS as SUPPORTED_ANA_TOOLS
from grapharrow.ana.catalog import get_tool_specs, validate_args
from grapharrow.ana.handles import EntityPayload, EntityReference, HandleStore
from grapharrow.ana.values import _int_arg, _primary_label
from grapharrow.ana.values import equal as _property_equal
from grapharrow.app import GraphArrowApplication
from grapharrow.use_cases import ana_patterns, ana_roles, ana_search
from grapharrow.use_cases.ana_read_cache import AnaReadCache
from grapharrow.use_cases.ana_resources import AnaLimits, AnaResourceBudget, AnaResources
from grapharrow.use_cases.ana_runtime import AnaRuntime
from grapharrow.use_cases.ana_schema_cache import SnapshotSchema
from grapharrow.use_cases.graph import GraphTabularState


class GraphArrowAgentBackend(AnaRuntime):
    """One read-only workflow over an in-memory view of one published snapshot."""

    def __init__(
        self,
        application: GraphArrowApplication,
        *,
        handle_store: HandleStore | None = None,
        schema_entry: str = "overview",
        resource_budget: AnaResourceBudget | None = None,
    ) -> None:
        self._handle_store = handle_store if handle_store is not None else HandleStore()
        if self._handle_store.counter or self._handle_store.handles or self._handle_store.lineage:
            raise ValueError("A new ANA workflow requires an empty handle store")
        self._application = application
        self._schema_entry = schema_entry
        self._closed = False
        self._call_lock = RLock()
        self.last_fetch = None
        self._owner = self._handle_store.namespace
        # Pin canonical tables and projections together against concurrent vacuum.
        with application.use_cases.graph.writer_lock():
            self._snapshot_state: GraphTabularState | None = (
                application.use_cases.graph.load_tabular_state()
            )
            node_ids = self._state.node_table["node_id"].to_pylist()
            label_values = self._state.node_table["labels"].to_pylist()
            labels = dict.fromkeys(label for values in label_values for label in values)
            self._property_tables = {
                label: application.use_cases.graph.load_node_property_table(
                    snapshot=self._state.snapshot, label=label
                )
                for label in labels
            }
        self._positions = {int(value): index for index, value in enumerate(node_ids)}
        self._label_ids: dict[str, list[int]] = defaultdict(list)
        for node_id, labels in zip(
            node_ids,
            label_values,
            strict=True,
        ):
            for label in labels:
                self._label_ids[label].append(node_id)
        self._schema_snapshot: SnapshotSchema | None = application._ana_schema_cache.get_or_create(
            self._state.snapshot, self._discover_schema
        )
        # Responses can expose nested schema values to native callers.
        self._schema = deepcopy(self._schema_snapshot.schema)
        settings = application.settings
        adjacency_bytes = GraphAdjacency.reservation(
            self._state.edge_table.num_rows,
            self._schema["relationship_types"],
            settings.ana_adjacency_index_max_bytes_per_session,
            node_count=self._state.node_table.num_rows,
        )
        self._adjacency = (
            application.use_cases.graph.adjacency_for_state(self._state)
            if adjacency_bytes
            else None
        )
        if (
            self._adjacency is not None
            and self._adjacency.reserved_bytes <= settings.ana_adjacency_index_max_bytes_per_session
        ):
            adjacency_bytes = self._adjacency.reserved_bytes
        else:
            self._adjacency = None
            adjacency_bytes = 0
        self._read_cache = (
            AnaReadCache(settings.ana_read_cache_max_bytes_per_session)
            if settings.ana_read_cache_enabled
            else None
        )
        self._resources = AnaResources(
            AnaLimits(
                max_handles=settings.ana_max_handles_per_session,
                max_rows=settings.ana_max_rows_per_session,
                max_bytes=settings.ana_max_bytes_per_session,
            ),
            resource_budget
            if resource_budget is not None
            else AnaResourceBudget(settings.ana_max_total_bytes),
            (
                self._snapshot_state,
                self._property_tables,
                self._positions,
                self._label_ids,
            ),
            shared_state=(self._schema_snapshot, self._schema),
            reserved_bytes=self._read_cache.max_bytes if self._read_cache is not None else 0,
        )
        if adjacency_bytes and not self._resources.reserve_snapshot_capacity(adjacency_bytes):
            self._adjacency = None

    @property
    def resource_usage(self) -> dict[str, int]:
        """Conservative retained-state accounting, for local diagnostics and benchmarks."""
        with self._call_lock:
            usage = self._resources.usage
            if self._read_cache is not None:
                usage.update(
                    cache_used_bytes=self._read_cache.used_bytes,
                    cache_reserved_bytes=self._read_cache.max_bytes,
                    cache_hits=self._read_cache.hits,
                    cache_misses=self._read_cache.misses,
                )
            return usage

    def _cached_read(self, key: tuple[Any, ...], read: Callable[[], Any]) -> Any:
        if self._read_cache is None:
            return read()
        return self._read_cache.get_or_create(key, read)

    def call(self, tool: str, args: dict[str, Any]) -> dict[str, Any]:
        """Validate the call and record lineage in the workflow's store."""
        with self._call_lock:
            if self._closed:
                raise RuntimeError("Graph backend is closed")
            if tool not in SUPPORTED_ANA_TOOLS:
                raise ValueError(f"Tool is not implemented by the GraphArrow ANA backend: {tool}")
            store = self._handle_store
            previous_fetch = self.last_fetch
            try:
                with store.track_changes() as changes, self._resources.track_changes():
                    args = validate_args(tool, args)
                    operation = getattr(self, "_" + tool)
                    result = operation(args)
                    store.record_lineage(tool, args, result)
                    touched = set(changes.created)
                    if result.get("handle") in store.handles:
                        touched.add(result["handle"])
                    # Build the response before committing final resource admission.
                    response = dict(result)
                    self._resources.finalize(store, touched, self.last_fetch)
                    return response
            except BaseException:
                self.last_fetch = previous_fetch
                raise

    def _store(
        self,
        rows: list[dict[str, Any]],
        *,
        focus: str | None = None,
        kind: str = "rows",
        columns: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row_bytes, size = self._resources.reserve_handle(rows, (focus, kind, columns, metadata))
        result = self._handle_store.store(
            rows, focus=focus, kind=kind, columns=columns, metadata=metadata
        )
        self._resources.register_handle(str(result["handle"]), len(rows), row_bytes, size)
        return dict(result)

    def tool_specs(self, *, enable_planning_tools: bool = False) -> list[dict[str, Any]]:
        """Return locally maintained schemas for the implemented tools."""
        return get_tool_specs(
            schema_entry=self._schema_entry, enable_planning_tools=enable_planning_tools
        )

    def __enter__(self) -> GraphArrowAgentBackend:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        """Release all workflow state; repeated close is harmless."""
        with self._call_lock:
            if not self._closed:
                self._closed = True
                self._handle_store.clear()
                self._resources.close()
                self.last_fetch = None
                self._property_tables.clear()
                self._positions.clear()
                self._label_ids.clear()
                self._schema.clear()
                self._schema_snapshot = None
                self._snapshot_state = None
                self._adjacency = None
                if self._read_cache is not None:
                    self._read_cache.clear()
                    self._read_cache = None

    @property
    def _state(self) -> GraphTabularState:
        if self._snapshot_state is None:
            raise RuntimeError("Graph backend is closed")
        return self._snapshot_state

    def _entity_query_metadata(self, node_ids: set[int], var: str, label: str) -> dict[str, Any]:
        """Preserve full branch membership separately from limited entity rows."""
        return {"native_entity_query": {"node_ids": node_ids, "var": var, "label": label}}

    def _combined_server_side_entity_metadata(
        self, *, op: str, left: Any, right: Any, left_var: str, right_var: str, out_var: str
    ) -> dict[str, Any] | None:
        if op != "union":
            return None
        branches = []
        for handle, variable in ((left, left_var), (right, right_var)):
            branch = handle.metadata.get("native_entity_query") or handle.metadata.get(
                "native_entity_set"
            )
            if not branch or branch["var"] != variable:
                return None
            branches.append(branch)
        if branches[0]["label"] != branches[1]["label"]:
            return None
        return {
            "native_entity_set": {
                "var": out_var,
                "label": branches[0]["label"],
                "node_ids": set(branches[0]["node_ids"]) | set(branches[1]["node_ids"]),
            }
        }

    def _server_side_entity_set_count(self, handle: Any, *, var: Any, distinct: bool) -> int | None:
        branch = handle.metadata.get("native_entity_set")
        if distinct and var and branch and branch["var"] == str(var):
            return len(branch["node_ids"])
        return None

    def _node(self, node_id: int) -> EntityPayload:
        row = self._state.node_table.slice(self._positions[node_id], 1).to_pylist()[0]
        return self._node_payload(row)

    def _node_payload(self, row: dict[str, Any]) -> EntityPayload:
        payload = EntityPayload(
            labels=list(row["labels"]), properties=json.loads(row["properties_json"])
        )
        payload.reference = EntityReference(self._owner, str(row["node_id"]))
        return payload

    def _materialize_nodes(self, node_ids: list[int]) -> list[EntityPayload]:
        if not node_ids:
            return []
        positions = pa.array([self._positions[node_id] for node_id in node_ids], type=pa.int64())
        columns = self._state.node_table.select(["node_id", "labels", "properties_json"])
        return [self._node_payload(row) for row in columns.take(positions).to_pylist()]

    def _nodes(self, label: str) -> Any:
        ids = pa.array(self._label_ids.get(label, []), type=pa.int64())
        return self._state.node_table.filter(pc.is_in(self._state.node_table["node_id"], ids))

    def _node_search(self, args: dict[str, Any]) -> dict[str, Any]:
        label = str(args["label"])
        prop = str(args.get("property", "name"))
        var = str(args.get("as") or label.lower())
        limit = _int_arg(args, "limit", 1000, min_value=1)
        value = args["value"]
        if value is None:
            return dict(self._store([], focus=var))
        table = self._property_tables.get(label)
        if (
            table is not None
            and prop in table.column_names
            and "node_id" in table.column_names
            and pa.types.is_int64(table["node_id"].type)
            and not table.schema.field("node_id").metadata
        ):
            try:
                selected = self._search_projected(label, table, prop, value)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
                selected = self._search_canonical(label, prop, value)["node_id"]
        else:
            selected = self._search_canonical(label, prop, value)["node_id"]
        node_ids = selected.slice(0, limit).to_pylist()
        rows = [{var: entity, "__rel_ids": []} for entity in self._materialize_nodes(node_ids)]
        return dict(self._store(rows, focus=var))

    def _search_projected(
        self, label: str, table: pa.Table, prop: str, value: Any
    ) -> pa.Array | pa.ChunkedArray:
        indexed = self._application.use_cases.graph.lookup_node_property_table(
            snapshot=self._state.snapshot,
            catalog=self._state.catalog,
            label=label,
            equalities={prop: value},
            table=table,
        )
        matches = (
            indexed["node_id"]
            if indexed is not None
            else pc.filter(table["node_id"], pc.equal(table[prop], value))
        )
        if len(matches) <= 1 and pa.types.is_int64(matches.type):
            if len(matches) == 0:
                return matches
            position = self._positions.get(matches[0].as_py())
            if position is not None and label in self._state.node_table["labels"][position].as_py():
                return matches
            return matches.slice(0, 0)

        # Keep canonical node order when a projection has several matches.
        candidates = pa.array(self._label_ids.get(label, []), type=pa.int64())
        return pc.filter(candidates, pc.is_in(candidates, matches))

    def _search_canonical(self, label: str, prop: str, value: Any) -> Any:
        table = self._nodes(label)
        mask = pa.array(
            [
                _property_equal(json.loads(item.as_py()).get(prop), value)
                for item in table["properties_json"]
            ],
            type=pa.bool_(),
        )
        return table.filter(mask)

    def _node_scan(self, args: dict[str, Any]) -> dict[str, Any]:
        label = str(args["label"])
        var = str(args.get("as") or label.lower())
        limit = _int_arg(args, "limit", 50000, min_value=1)
        if limit > 5000 and not args.get("allow_large"):
            raise ValueError("node_scan is capped at 5000 unless allow_large=true")
        node_ids = self._label_ids.get(label, [])[:limit]
        rows = [{var: entity, "__rel_ids": []} for entity in self._materialize_nodes(node_ids)]
        return dict(self._store(rows, focus=var))

    def _count_nodes(self, args: dict[str, Any]) -> dict[str, Any]:
        label = str(args["label"])
        alias = str(args.get("alias") or f"{label.lower()}_count")
        return dict(
            self._store(
                [{alias: len(self._label_ids.get(label, []))}], kind="table", columns=[alias]
            )
        )

    def _traversal_edges(self, rel_type: str, *, properties: bool = False) -> pa.Table | None:
        """Prepare the scan fallback once per expansion or pattern hop."""
        if self._adjacency is not None:
            return None
        edges = self._state.edge_table
        if not properties:
            edges = edges.select(["edge_id", "rel_type", "src_id", "dst_id"])
        return edges.filter(pc.equal(edges["rel_type"], rel_type))

    def _edges_for_sources(
        self,
        rel_type: str,
        node_ids: list[int],
        columns: tuple[str, ...],
        *,
        properties: bool = False,
        batch: bool = True,
        max_batch_rows: int | None = None,
        scan_edges: pa.Table | None = None,
    ) -> Iterator[Iterable[dict[str, Any]]]:
        edges = self._state.edge_table
        if self._adjacency is not None:
            return self._adjacency.rows_for_sources(
                edges, rel_type, node_ids, columns, properties=properties
            )
        if scan_edges is not None:
            edges = scan_edges
        else:
            edges = self._traversal_edges(rel_type, properties=properties)
            assert edges is not None
        return ana_patterns.edges_for_sources(
            edges, node_ids, columns, batch=batch, max_batch_rows=max_batch_rows
        )

    def _traversal_targets(
        self,
        rel_type: str,
        node_ids: list[int],
        columns: tuple[str, ...],
        allowed: set[int],
        scan_edges: pa.Table | None = None,
    ) -> set[int]:
        if not node_ids:
            return set()
        edges = self._state.edge_table
        if self._adjacency is not None:
            return self._adjacency.targets(edges, rel_type, node_ids, columns, allowed)
        if scan_edges is not None:
            edges = scan_edges
        else:
            edges = self._traversal_edges(rel_type)
            assert edges is not None
        sources = pa.array(node_ids, type=pa.int64())
        targets: set[int] = set()
        for column in columns:
            target = "dst_id" if column == "src_id" else "src_id"
            selected = pc.filter(edges[target], pc.is_in(edges[column], sources))
            targets.update(set(pc.unique(selected).to_pylist()) & allowed)
        return targets

    def _expand(self, args: dict[str, Any]) -> dict[str, Any]:
        handle = self._handle(str(args["from"]))
        source = str(args["source"])
        rel_type = str(args["relationship_type"])
        label = str(args["target_label"])
        var = str(args.get("as") or label.lower())
        direction = str(args.get("direction", "out"))
        if direction not in {"in", "out", "both"}:
            raise ValueError(f"Unsupported direction: {direction}")
        if rel_type not in self._schema["relationship_types"]:
            raise ValueError(f"Unknown relationship_type {rel_type!r}")
        limit = _int_arg(args, "limit", 10000, min_value=1)
        page_size = _int_arg(args, "page_size", 2000, min_value=1)
        self._guard_large_handle_input(
            handle,
            tool="expand",
            var=source,
            args=args,
            alternatives=["use a selective node_search"],
        )
        scan_edges = self._traversal_edges(rel_type)
        targets = set(self._label_ids.get(label, []))
        output = []
        inputs = [row for row in handle.rows if source in row]
        node_ids = [ana_patterns.source_node_id(row[source], self._owner) for row in inputs]
        source_columns = (
            ("src_id",)
            if direction == "out"
            else ("dst_id",)
            if direction == "in"
            else ("src_id", "dst_id")
        )
        matches_by_source = self._edges_for_sources(
            rel_type,
            node_ids,
            source_columns,
            batch=limit >= len(node_ids),
            max_batch_rows=max(1024, limit),
            scan_edges=scan_edges,
        )
        processed = 0
        for row, node_id, selected in zip(inputs, node_ids, matches_by_source, strict=True):
            processed += 1
            for edge in selected:
                edge_key = str(edge["edge_id"])
                if edge_key in row.get("__rel_ids", []):
                    continue
                target_id = edge["dst_id"] if edge["src_id"] == node_id else edge["src_id"]
                if target_id not in targets:
                    continue
                output.append(
                    {
                        **row,
                        var: self._node(target_id),
                        "__rel_ids": [*row.get("__rel_ids", []), edge_key],
                    }
                )
                if len(output) >= limit:
                    break
            if len(output) >= limit:
                break
        # The reference keeps a full query branch for union counts. Its branch
        # uses input entities independently of the materialized traversal limit.
        full_targets = self._traversal_targets(
            rel_type, node_ids, source_columns, targets, scan_edges
        )
        metadata = self._entity_query_metadata(full_targets, var, label) if inputs else None
        result = self._store(output, focus=var, metadata=metadata)
        result.update(
            pages=(processed + page_size - 1) // page_size,
            page_size=page_size,
            input_count=len(inputs),
            limited=len(output) >= limit,
        )
        return dict(result)

    def _entity_resolve(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_search.entity_resolve(self, args)

    def _value_search(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_search.value_search(self, args)

    def _pattern_query(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_patterns.pattern_query(self, args)

    def _multi_hop_query(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_patterns.pattern_query(self, args, multi_hop=True)

    def _relationship_query(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_patterns.relationship_query(self, args)

    def _optional_expand_count(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_patterns.optional_expand_count(self, args)

    def _optional_count_by_pattern(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_patterns.optional_count_by_pattern(self, args)

    def _expand_aggregate(self, args: dict[str, Any]) -> dict[str, Any]:
        source = str(args["source"])
        handle = self._handle(str(args["from"]))
        entities = [row[source] for row in handle.rows if source in row]
        if not entities:
            return dict(self._store([], kind="table", columns=[]))
        label = _primary_label(entities[0]["labels"])
        if not args.get("group_by") and not args.get("metrics"):
            raise ValueError("expand_aggregate requires group_by and/or metrics")
        return self._multi_hop_query(
            {
                **args,
                "start_label": label,
                "start_as": source,
                "hops": [
                    {
                        "relationship_type": args["relationship_type"],
                        "direction": args["direction"],
                        "target_label": args["target_label"],
                        "as": args.get("as") or str(args["target_label"]).lower(),
                    }
                ],
                "limit": _int_arg(args, "limit", 1000, min_value=1),
            }
        )

    def _top_entities_by_property(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_patterns.top_entities_by_property(self, args)

    def _constraint_query(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_roles.constraint_query(self, args)

    def _same_target_role_intersection(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_roles.same_target_role_intersection(self, args)

    def _shared_role_aggregate(self, args: dict[str, Any]) -> dict[str, Any]:
        return ana_roles.shared_role_aggregate(self, args)

    def _label_counts(self, labels: list[str]) -> dict[str, int]:
        return {label: len(self._label_ids.get(label, [])) for label in labels}

    def _node_property_samples(self, label: str, prop: str, *, limit: int = 3) -> list[Any]:
        samples = []
        for item in self._nodes(label)["properties_json"]:
            value = json.loads(item.as_py()).get(prop)
            if value is not None and value not in samples:
                samples.append(value)
            if len(samples) >= limit:
                break
        return samples

    def _relationship_property_samples(
        self, rel_type: str, prop: str, *, limit: int = 3
    ) -> list[Any]:
        samples = []
        edges = self._state.edge_table
        for item in edges.filter(pc.equal(edges["rel_type"], rel_type))["properties_json"]:
            value = json.loads(item.as_py()).get(prop)
            if value is not None and value not in samples:
                samples.append(value)
            if len(samples) >= limit:
                break
        return samples

    def _discover_schema(self) -> dict[str, Any]:
        node_props: dict[tuple[str, str], set[str]] = defaultdict(set)
        edge_props: dict[tuple[str, str], set[str]] = defaultdict(set)
        node_values: dict[tuple[str, str], int] = defaultdict(int)
        edge_values: dict[tuple[str, str], int] = defaultdict(int)
        relationship_counts: dict[str, int] = defaultdict(int)
        patterns = set()
        labels_by_id = {}
        for node_id, labels, raw in zip(
            self._state.node_table["node_id"].to_pylist(),
            self._state.node_table["labels"].to_pylist(),
            self._state.node_table["properties_json"].to_pylist(),
            strict=True,
        ):
            labels_by_id[node_id] = labels
            if raw == "{}":
                continue
            for prop, value in json.loads(raw).items():
                property_type = _property_type(value)
                for label in labels:
                    node_props[label, prop].add(property_type)
                    if value is not None:
                        node_values[label, prop] += 1
        edges = self._state.edge_table
        for rel, source, target, raw in zip(
            edges["rel_type"].to_pylist(),
            edges["src_id"].to_pylist(),
            edges["dst_id"].to_pylist(),
            edges["properties_json"].to_pylist(),
            strict=True,
        ):
            relationship_counts[rel] += 1
            if raw != "{}":
                for prop, value in json.loads(raw).items():
                    edge_props[rel, prop].add(_property_type(value))
                    if value is not None:
                        edge_values[rel, prop] += 1
            for src in labels_by_id.get(source, []):
                for dst in labels_by_id.get(target, []):
                    patterns.add((src, rel, dst))
        return {
            "labels": sorted(self._label_ids),
            "relationship_types": sorted(relationship_counts),
            "node_properties": [
                {
                    "nodeLabels": [label],
                    "propertyName": prop,
                    "propertyTypes": sorted(types),
                    "mandatory": node_values[label, prop] == len(self._label_ids[label]),
                }
                for (label, prop), types in sorted(node_props.items())
            ],
            "relationship_properties": [
                {
                    "relType": rel,
                    "propertyName": prop,
                    "propertyTypes": sorted(types),
                    "mandatory": edge_values[rel, prop] == relationship_counts[rel],
                }
                for (rel, prop), types in sorted(edge_props.items())
            ],
            "label_relationships": [
                {"source_label": src, "relationship_type": rel, "target_label": dst}
                for src, rel, dst in sorted(patterns)
            ],
        }


def _property_type(value: Any) -> str:
    if isinstance(value, list):
        element = _property_type(next((item for item in value if item is not None), None))
        return f"List<{element}>"
    return {bool: "Boolean", int: "Integer", float: "Float", str: "String"}.get(type(value), "Any")
