"""Backward-compatible MCP interface imports.

The current public MCP surface is Cypher-only. The implementation lives in
``mcp_cypher`` so future agent-first MCP adapters can grow separately without
changing existing imports.
"""

from __future__ import annotations

from grapharrow.interfaces.mcp_cypher import (
    DEFAULT_MCP_QUERY_MAX_HOPS,
    DEFAULT_MCP_QUERY_MAX_ROWS,
    MAX_SUPPORTED_MCP_QUERY_ROWS,
    GraphArrowMcpCypherInterface,
    GraphArrowMcpInterface,
    McpToolDefinition,
    create_mcp_cypher_interface,
    create_mcp_interface,
)

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
