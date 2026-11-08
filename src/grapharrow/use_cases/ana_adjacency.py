"""Compatibility import for the former native ANA adjacency helper."""

from grapharrow.adapters.adjacency import GraphAdjacency as AnaAdjacency

__all__ = ["AnaAdjacency"]
