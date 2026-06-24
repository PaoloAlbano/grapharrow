"""Compatibility re-export for the exception hierarchy.

The project is moving core exceptions into ``grapharrow.core``.
"""

from grapharrow.core.errors import (
    CatalogError,
    GraphArrowError,
    NotFoundError,
    QueryError,
    SchemaError,
    StorageError,
    TransactionError,
)

__all__ = [
    "CatalogError",
    "GraphArrowError",
    "NotFoundError",
    "QueryError",
    "SchemaError",
    "StorageError",
    "TransactionError",
]
