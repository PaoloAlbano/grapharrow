"""Compatibility wrapper for the openCypher parser adapter."""

from grapharrow.adapters.opencypher import OpenCypherAdapter, parse_cypher

__all__ = ["OpenCypherAdapter", "parse_cypher"]
