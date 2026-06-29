"""Transport-agnostic Cypher-only MCP interface for GraphArrow."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from grapharrow.adapters import FileSystemAdapter
from grapharrow.app import GraphArrowApplication, build_application
from grapharrow.config import GraphArrowSettings
from grapharrow.core.errors import QueryError, TransactionError
from grapharrow.domain import DatasetFile
from grapharrow.interfaces.cypher_guard import query_requires_write

DEFAULT_MCP_QUERY_MAX_ROWS = 100
DEFAULT_MCP_QUERY_MAX_HOPS = 5
MAX_SUPPORTED_MCP_QUERY_ROWS = 1000
_VARIABLE_LENGTH_RE = re.compile(r"\*\s*(\d+)(?:\s*\.\.\s*(\d+))?")


@dataclass(frozen=True, slots=True)
class McpToolDefinition:
    """Metadata for a single MCP-exposed tool."""

    name: str
    description: str
    input_schema: dict[str, object]


@dataclass(frozen=True, slots=True)
class GraphArrowMcpCypherInterface:
    """Cypher-oriented MCP surface that can be mounted behind any transport."""

    grapharrow: GraphArrowApplication
    tools: tuple[McpToolDefinition, ...]

    def list_tools(self) -> tuple[McpToolDefinition, ...]:
        """Return the Cypher MCP tool definitions exposed by this interface."""

        return self.tools

    def call_tool(
        self,
        name: str,
        arguments: dict[str, object] | None = None,
    ) -> dict[str, object]:
        """Dispatch a Cypher MCP tool call by name."""

        payload = dict(arguments or {})
        if name == "query":
            return self.query(
                query=_require_string(payload, "query"),
                params=_optional_params(payload),
                max_rows=_optional_int(payload, "max_rows"),
                max_hops=_optional_int(payload, "max_hops"),
            )
        if name == "write_cypher":
            return self.write_cypher(
                query=_require_string(payload, "query"),
                params=_optional_params(payload),
            )
        if name == "get_schema":
            return self.get_schema()
        if name == "get_constraints":
            return self.get_constraints()
        if name == "get_indexes":
            return self.get_indexes()
        raise QueryError(f"Unknown MCP tool: {name}")

    def query(
        self,
        *,
        query: str,
        params: dict[str, object] | None = None,
        max_rows: int | None = None,
        max_hops: int | None = None,
    ) -> dict[str, object]:
        """Execute a read-only Cypher query."""

        if query_requires_write(query):
            raise TransactionError("The MCP query tool only supports read-only Cypher.")
        resolved_max_rows = _resolve_max_rows(max_rows)
        resolved_max_hops = _resolve_max_hops(max_hops)
        _enforce_max_hops(query, resolved_max_hops)
        result = self.grapharrow.use_cases.cypher.execute(query, dict(params or {}))
        return _query_result_payload(
            result.columns,
            result.rows,
            max_rows=resolved_max_rows,
            tool_name="query",
        )

    def write_cypher(
        self,
        *,
        query: str,
        params: dict[str, object] | None = None,
    ) -> dict[str, object]:
        """Execute a write-capable Cypher query."""

        result = self.grapharrow.use_cases.cypher.execute(query, dict(params or {}))
        return _query_result_payload(
            result.columns,
            result.rows,
            tool_name="write_cypher",
        )

    def get_schema(self) -> dict[str, object]:
        """Return the current catalog schema as an MCP-friendly payload."""

        catalog = self.grapharrow.repositories.catalog.load()
        return {
            "catalog_version": catalog.version.value,
            "labels": [
                {"id": definition.id.value, "name": definition.name}
                for definition in catalog.labels
            ],
            "relationship_types": [
                {"id": definition.id.value, "name": definition.name}
                for definition in catalog.relationship_types
            ],
            "properties": [
                {
                    "owner_kind": definition.owner_kind,
                    "owner_name": definition.owner_name,
                    "name": definition.name,
                    "type": definition.data_type,
                    "nullable": definition.nullable,
                    "deprecated": definition.deprecated,
                }
                for definition in catalog.properties
            ],
        }

    def get_constraints(self) -> dict[str, object]:
        """Return the current logical constraints from the catalog."""

        catalog = self.grapharrow.repositories.catalog.load()
        return {
            "catalog_version": catalog.version.value,
            "constraints": [
                {
                    "kind": definition.kind,
                    "owner_kind": definition.owner_kind,
                    "owner_name": definition.owner_name,
                    "property_name": definition.property_name,
                    "source_labels": list(definition.source_labels),
                    "target_labels": list(definition.target_labels),
                }
                for definition in catalog.constraints
            ],
        }

    def get_indexes(self) -> dict[str, object]:
        """Expose derived dataset files that act as bootstrap indexes."""

        manifest = self.grapharrow.repositories.snapshot.load_manifest()
        catalog = self.grapharrow.repositories.catalog.load()
        return {
            "catalog_version": catalog.version.value,
            "snapshot_id": manifest.snapshot_id.value,
            "node_labels": [
                {
                    "label": _label_from_node_label_file(dataset_file),
                    **_dataset_file_payload(dataset_file),
                }
                for dataset_file in manifest.node_labels
            ],
            "node_properties": [
                {
                    "label": _label_from_node_property_file(dataset_file),
                    **_dataset_file_payload(dataset_file),
                }
                for dataset_file in manifest.node_properties
            ],
            "adjacency_by_source": [
                {
                    "relationship_type": _rel_type_from_adjacency_file(dataset_file),
                    **_dataset_file_payload(dataset_file),
                }
                for dataset_file in manifest.adjacency_by_source
            ],
            "adjacency_by_dest": [
                {
                    "relationship_type": _rel_type_from_adjacency_file(dataset_file),
                    **_dataset_file_payload(dataset_file),
                }
                for dataset_file in manifest.adjacency_by_dest
            ],
            "edge_properties": [
                {
                    "relationship_type": _rel_type_from_edge_property_file(dataset_file),
                    **_dataset_file_payload(dataset_file),
                }
                for dataset_file in manifest.edge_properties
            ],
        }


GraphArrowMcpInterface = GraphArrowMcpCypherInterface


def create_mcp_cypher_interface(
    path: str | Path,
    *,
    settings: GraphArrowSettings | None = None,
    filesystem: FileSystemAdapter | None = None,
) -> GraphArrowMcpCypherInterface:
    """Build the Cypher-only GraphArrow MCP interface for a single database path."""

    container = build_application(path, settings=settings, filesystem=filesystem)
    container.use_cases.admin.initialize()
    return GraphArrowMcpCypherInterface(
        grapharrow=container,
        tools=_cypher_tools(),
    )


def create_mcp_interface(
    path: str | Path,
    *,
    settings: GraphArrowSettings | None = None,
    filesystem: FileSystemAdapter | None = None,
) -> GraphArrowMcpCypherInterface:
    """Build the backward-compatible Cypher-only MCP interface."""

    return create_mcp_cypher_interface(path, settings=settings, filesystem=filesystem)


def _cypher_tools() -> tuple[McpToolDefinition, ...]:
    """Return the fixed Cypher-only MCP tool definitions."""

    return (
        McpToolDefinition(
            name="query",
            description="Execute a read-only Cypher query.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "params": {"type": "object"},
                    "max_rows": {"type": "integer", "minimum": 1, "maximum": 1000},
                    "max_hops": {"type": "integer", "minimum": 1, "maximum": 5},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        ),
        McpToolDefinition(
            name="write_cypher",
            description="Execute a write-capable Cypher query.",
            input_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "params": {"type": "object"},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        ),
        McpToolDefinition(
            name="get_schema",
            description="Return labels, relationship types, and property definitions.",
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        ),
        McpToolDefinition(
            name="get_constraints",
            description="Return logical constraint definitions from the current catalog.",
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        ),
        McpToolDefinition(
            name="get_indexes",
            description=(
                "Return derived node-label, property, and adjacency datasets for the snapshot."
            ),
            input_schema={
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        ),
    )


def _query_result_payload(
    columns: tuple[str, ...],
    rows: tuple[dict[str, Any], ...],
    *,
    max_rows: int | None = None,
    tool_name: str,
) -> dict[str, object]:
    """Shape a Cypher QueryResult into the MCP tabular response envelope."""

    resolved_rows = list(rows)
    returned_rows = resolved_rows[:max_rows] if max_rows is not None else resolved_rows
    truncated = max_rows is not None and len(resolved_rows) > max_rows
    return {
        "columns": list(columns),
        "rows": returned_rows,
        "row_count": len(resolved_rows),
        "returned_row_count": len(returned_rows),
        "truncated": truncated,
        "summary": {
            "tool": tool_name,
            "column_count": len(columns),
            "row_count": len(resolved_rows),
            "returned_row_count": len(returned_rows),
            "result_kind": _classify_result_kind(len(returned_rows)),
        },
        "warnings": (
            [(f"Result rows were truncated to the first {max_rows} rows for MCP consumption.")]
            if truncated
            else []
        ),
    }


def _optional_params(arguments: dict[str, object]) -> dict[str, object]:
    """Read optional Cypher parameters from a tool-call payload."""

    raw = arguments.get("params", {})
    if not isinstance(raw, dict):
        raise QueryError("The MCP params argument must be a JSON object.")
    return dict(raw)


def _optional_int(arguments: dict[str, object], key: str) -> int | None:
    """Read an optional integer guardrail from a tool-call payload."""

    value = arguments.get(key)
    if value is None:
        return None
    if not isinstance(value, int):
        raise QueryError(f"The MCP {key} argument must be an integer.")
    return value


def _require_string(arguments: dict[str, object], key: str) -> str:
    """Read a required non-empty string from a tool-call payload."""

    value = arguments.get(key)
    if not isinstance(value, str) or not value.strip():
        raise QueryError(f"The MCP {key} argument must be a non-empty string.")
    return value


def _resolve_max_rows(max_rows: int | None) -> int:
    """Resolve the effective row limit for read-only Cypher MCP queries."""

    resolved = DEFAULT_MCP_QUERY_MAX_ROWS if max_rows is None else max_rows
    if resolved < 1 or resolved > MAX_SUPPORTED_MCP_QUERY_ROWS:
        raise QueryError(
            f"The MCP max_rows argument must be between 1 and {MAX_SUPPORTED_MCP_QUERY_ROWS}."
        )
    return resolved


def _resolve_max_hops(max_hops: int | None) -> int:
    """Resolve the effective hop limit for read-only Cypher MCP queries."""

    resolved = DEFAULT_MCP_QUERY_MAX_HOPS if max_hops is None else max_hops
    if resolved < 1 or resolved > DEFAULT_MCP_QUERY_MAX_HOPS:
        raise QueryError(
            f"The MCP max_hops argument must be between 1 and {DEFAULT_MCP_QUERY_MAX_HOPS}."
        )
    return resolved


def _enforce_max_hops(query: str, max_hops: int) -> None:
    """Reject read-only MCP queries that exceed the configured hop limit."""

    longest_path_hops = _infer_longest_requested_hops(query)
    if longest_path_hops > max_hops:
        raise QueryError(f"The MCP query exceeds the configured hop limit of {max_hops}.")


def _infer_longest_requested_hops(query: str) -> int:
    """Infer the largest requested path length from the Cypher text."""

    variable_length_hops = 0
    for match in _VARIABLE_LENGTH_RE.finditer(query):
        exact_hops = int(match.group(1))
        upper_bound = int(match.group(2)) if match.group(2) is not None else exact_hops
        variable_length_hops = max(variable_length_hops, upper_bound)

    explicit_chain_hops = 0
    for line in query.splitlines():
        line_hops = line.count("->") + line.count("<-")
        explicit_chain_hops = max(explicit_chain_hops, line_hops)
    return max(variable_length_hops, explicit_chain_hops)


def _classify_result_kind(returned_row_count: int) -> str:
    """Classify the returned row count for MCP response summaries."""

    if returned_row_count == 0:
        return "empty"
    if returned_row_count == 1:
        return "single_row"
    return "multi_row"


def _dataset_file_payload(dataset_file: DatasetFile) -> dict[str, object]:
    """Serialize common manifest file metadata for MCP index inspection."""

    return {
        "path": dataset_file.path,
        "row_count": dataset_file.row_count,
        "checksum": dataset_file.checksum,
        "checksum_algorithm": dataset_file.checksum_algorithm,
        "min_key": dataset_file.min_key,
        "max_key": dataset_file.max_key,
    }


def _label_from_node_label_file(dataset_file: DatasetFile) -> str:
    """Extract the label name from a node-label derived dataset path."""

    parts = dataset_file.path.split("/")
    if len(parts) >= 3:
        return parts[-2]
    raise QueryError(f"Unexpected node label dataset path: {dataset_file.path}")


def _label_from_node_property_file(dataset_file: DatasetFile) -> str:
    """Extract the label name from a node-property derived dataset path."""

    parts = dataset_file.path.split("/")
    if len(parts) >= 3:
        return parts[-2]
    raise QueryError(f"Unexpected node property dataset path: {dataset_file.path}")


def _rel_type_from_adjacency_file(dataset_file: DatasetFile) -> str:
    """Extract the relationship type from an adjacency derived dataset path."""

    parts = dataset_file.path.split("/")
    if len(parts) >= 3:
        return parts[-2]
    raise QueryError(f"Unexpected adjacency dataset path: {dataset_file.path}")


def _rel_type_from_edge_property_file(dataset_file: DatasetFile) -> str:
    """Extract the relationship type from an edge-property derived dataset path."""

    parts = dataset_file.path.split("/")
    if len(parts) >= 3:
        return parts[-2]
    raise QueryError(f"Unexpected edge property dataset path: {dataset_file.path}")


__all__ = [
    "DEFAULT_MCP_QUERY_MAX_HOPS",
    "DEFAULT_MCP_QUERY_MAX_ROWS",
    "GraphArrowMcpCypherInterface",
    "GraphArrowMcpInterface",
    "MAX_SUPPORTED_MCP_QUERY_ROWS",
    "McpToolDefinition",
    "create_mcp_cypher_interface",
    "create_mcp_interface",
]
